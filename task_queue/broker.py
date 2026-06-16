import time
import logging
import threading
from typing import Optional, Dict, List, Callable
from dataclasses import dataclass

import redis

from .models import Task, TaskStatus, TaskPriority, TaskResult
from .cron import CronParser

logger = logging.getLogger(__name__)

KEY_TASK = "tq:task:{task_id}"
KEY_IDEMPOTENCY = "tq:idempotency:{key}"
KEY_QUEUE = "tq:queue:{priority}"
KEY_DELAYED = "tq:delayed"
KEY_PROCESSING = "tq:processing"
KEY_DEAD_LETTER = "tq:dead_letter"
KEY_RESULT = "tq:result:{task_id}"
KEY_RESULT_CHANNEL = "tq:result_channel"
KEY_CRON_TASKS = "tq:cron_tasks"
KEY_ALL_TASKS = "tq:all_tasks"

PRIORITY_ORDER = [TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]


class Broker:
    def __init__(self, redis_url: str = "redis://localhost:6379/0", namespace: str = "tq",
                 redis_client: Optional[redis.Redis] = None):
        if redis_client is not None:
            self.redis = redis_client
        else:
            self.redis = redis.Redis.from_url(redis_url, decode_responses=True)
        self.namespace = namespace
        self._stop_event = threading.Event()
        self._scheduler_thread: Optional[threading.Thread] = None
        self._reaper_thread: Optional[threading.Thread] = None
        self._cron_thread: Optional[threading.Thread] = None
        self._on_task_callback: Optional[Callable[[Task], None]] = None

    def _k(self, key: str) -> str:
        return f"{self.namespace}:{key}"

    def start_background_threads(self) -> None:
        if self._scheduler_thread is None or not self._scheduler_thread.is_alive():
            self._stop_event.clear()
            try:
                self._ensure_consistency()
            except Exception as e:
                logger.exception("Startup consistency check error: %s", e)
            self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self._scheduler_thread.start()
            self._reaper_thread = threading.Thread(target=self._reaper_loop, daemon=True)
            self._reaper_thread.start()
            self._cron_thread = threading.Thread(target=self._cron_loop, daemon=True)
            self._cron_thread.start()
            logger.info("Broker background threads started (consistency check completed)")

    def stop(self) -> None:
        self._stop_event.set()
        for t in [self._scheduler_thread, self._reaper_thread, self._cron_thread]:
            if t and t.is_alive():
                t.join(timeout=5)
        logger.info("Broker stopped")

    def save_task(self, task: Task) -> None:
        task.touch()
        pipe = self.redis.pipeline()
        pipe.set(self._k(KEY_TASK.format(task_id=task.task_id)), task.to_json())
        pipe.sadd(self._k(KEY_ALL_TASKS), task.task_id)
        pipe.execute()

    def get_task(self, task_id: str) -> Optional[Task]:
        data = self.redis.get(self._k(KEY_TASK.format(task_id=task_id)))
        if data is None:
            return None
        return Task.from_json(data)

    def check_idempotency(self, key: str) -> Optional[str]:
        if not key:
            return None
        return self.redis.get(self._k(KEY_IDEMPOTENCY.format(key=key)))

    def mark_idempotency(self, key: str, task_id: str, ttl_seconds: int = 86400 * 7) -> None:
        if not key:
            return
        self.redis.setex(self._k(KEY_IDEMPOTENCY.format(key=key)), ttl_seconds, task_id)

    def enqueue_task(self, task: Task) -> None:
        if task.idempotency_key:
            existing = self.check_idempotency(task.idempotency_key)
            if existing:
                logger.info("Task with idempotency key %s already exists as %s, skipping enqueue",
                            task.idempotency_key, existing)
                task.task_id = existing
                return
            self.mark_idempotency(task.idempotency_key, task.task_id)

        self.save_task(task)

        if task.status == TaskStatus.DELAYED:
            self.redis.zadd(self._k(KEY_DELAYED), {task.task_id: task.scheduled_at})
            logger.info("Task %s added to delayed queue, scheduled at %f", task.task_id, task.scheduled_at)
        elif task.status == TaskStatus.READY:
            queue_key = self._k(KEY_QUEUE.format(priority=int(task.priority)))
            self.redis.lpush(queue_key, task.task_id)
            logger.info("Task %s enqueued with priority %s", task.task_id, task.priority.name)

        if task.cron_expression:
            self.redis.hset(self._k(KEY_CRON_TASKS), task.task_id, task.cron_expression)

    def _scheduler_loop(self) -> None:
        logger.info("Delayed task scheduler started")
        while not self._stop_event.is_set():
            try:
                self._process_delayed_tasks()
            except Exception as e:
                logger.exception("Scheduler loop error: %s", e)
            self._stop_event.wait(0.5)

    def _ensure_consistency(self) -> None:
        logger.info("Starting consistency check across all tasks...")
        all_task_ids = self.redis.smembers(self._k(KEY_ALL_TASKS))

        ready_ids = set()
        for priority in PRIORITY_ORDER:
            queue_key = self._k(KEY_QUEUE.format(priority=int(priority)))
            ready_ids.update(self.redis.lrange(queue_key, 0, -1))

        delayed_ids = set(self.redis.zrange(self._k(KEY_DELAYED), 0, -1))
        processing_ids = set(self.redis.zrange(self._k(KEY_PROCESSING), 0, -1))
        dead_letter_ids = set(self.redis.lrange(self._k(KEY_DEAD_LETTER), 0, -1))

        fixed_count = 0
        now = time.time()

        for task_id in all_task_ids:
            task = self.get_task(task_id)
            if task is None:
                continue

            if task.status == TaskStatus.DELAYED:
                if task.scheduled_at <= now:
                    task.status = TaskStatus.READY
                    self.save_task(task)
                    queue_key = self._k(KEY_QUEUE.format(priority=int(task.priority)))
                    self.redis.lpush(queue_key, task_id)
                    self.redis.zrem(self._k(KEY_DELAYED), task_id)
                    fixed_count += 1
                    logger.info("[consistency] Task %s was DELAYED but expired, moved to ready queue", task_id)
                elif task_id not in delayed_ids:
                    self.redis.zadd(self._k(KEY_DELAYED), {task_id: task.scheduled_at})
                    fixed_count += 1
                    logger.info("[consistency] Task %s was DELAYED but missing from ZSet, restored", task_id)

            elif task.status == TaskStatus.READY:
                if task_id not in ready_ids:
                    queue_key = self._k(KEY_QUEUE.format(priority=int(task.priority)))
                    self.redis.lpush(queue_key, task_id)
                    fixed_count += 1
                    logger.info("[consistency] Task %s was READY but missing from queue, restored", task_id)

            elif task.status == TaskStatus.PROCESSING:
                if task_id not in processing_ids:
                    task.retry_count += 1
                    task.error_message = "recovered from crash during processing"
                    logger.warning("[consistency] Task %s was PROCESSING but missing from ZSet, retry %d/%d",
                                   task_id, task.retry_count, task.max_retries)
                    self._handle_retry_or_dead_letter(task, task.error_message)
                    fixed_count += 1

            elif task.status == TaskStatus.DEAD_LETTER:
                if task_id not in dead_letter_ids:
                    self.redis.lpush(self._k(KEY_DEAD_LETTER), task_id)
                    fixed_count += 1
                    logger.info("[consistency] Task %s was DEAD_LETTER but missing from DLQ, restored", task_id)

        logger.info("Consistency check completed, fixed %d / %d tasks", fixed_count, len(all_task_ids))

    def _process_delayed_tasks(self) -> None:
        now = time.time()
        pipe = self.redis.pipeline()
        pipe.zrangebyscore(self._k(KEY_DELAYED), 0, now)
        pipe.zremrangebyscore(self._k(KEY_DELAYED), 0, now)
        results, _ = pipe.execute()

        for task_id in results:
            task = self.get_task(task_id)
            if task is None:
                continue
            task.status = TaskStatus.READY
            self.save_task(task)
            queue_key = self._k(KEY_QUEUE.format(priority=int(task.priority)))
            self.redis.lpush(queue_key, task_id)
            logger.info("Delayed task %s moved to ready queue", task_id)

    def _reaper_loop(self) -> None:
        logger.info("Visibility timeout reaper started")
        while not self._stop_event.is_set():
            try:
                self._process_timed_out_tasks()
            except Exception as e:
                logger.exception("Reaper loop error: %s", e)
            self._stop_event.wait(1.0)

    def _process_timed_out_tasks(self) -> None:
        now = time.time()
        pipe = self.redis.pipeline()
        pipe.zrangebyscore(self._k(KEY_PROCESSING), 0, now)
        pipe.zremrangebyscore(self._k(KEY_PROCESSING), 0, now)
        timed_out_ids, _ = pipe.execute()

        for task_id in timed_out_ids:
            task = self.get_task(task_id)
            if task is None:
                continue
            task.retry_count += 1
            task.error_message = "visibility timeout exceeded"
            logger.warning("Task %s timed out (worker %s), retry %d/%d",
                           task_id, task.worker_id, task.retry_count, task.max_retries)
            self._handle_retry_or_dead_letter(task, "visibility timeout exceeded")

    def fetch_task(self, worker_id: str, timeout_seconds: int = 300) -> Optional[Task]:
        for priority in PRIORITY_ORDER:
            queue_key = self._k(KEY_QUEUE.format(priority=int(priority)))
            task_id = self.redis.rpop(queue_key)
            if task_id:
                task = self.get_task(task_id)
                if task is None:
                    continue
                task.status = TaskStatus.PROCESSING
                task.worker_id = worker_id
                task.started_at = time.time()
                task.visibility_timeout = timeout_seconds
                self.save_task(task)
                self.redis.zadd(self._k(KEY_PROCESSING), {task_id: time.time() + timeout_seconds})
                logger.info("Worker %s fetched task %s (priority=%s)", worker_id, task_id, priority.name)
                return task
        return None

    def renew_lease(self, task_id: str, worker_id: str, timeout_seconds: int = 300) -> bool:
        task = self.get_task(task_id)
        if task is None or task.worker_id != worker_id or task.status != TaskStatus.PROCESSING:
            return False
        self.redis.zadd(self._k(KEY_PROCESSING), {task_id: time.time() + timeout_seconds})
        return True

    def complete_task(self, task: Task, result: TaskResult) -> None:
        task.status = TaskStatus.SUCCESS if result.success else TaskStatus.FAILED
        task.finished_at = time.time()
        task.error_message = result.error
        self.save_task(task)
        self.redis.zrem(self._k(KEY_PROCESSING), task.task_id)
        self.redis.set(self._k(KEY_RESULT.format(task_id=task.task_id)), result.to_json())
        self.redis.publish(self._k(KEY_RESULT_CHANNEL), result.to_json())
        logger.info("Task %s completed: success=%s", task.task_id, result.success)

        if result.success and task.cron_expression:
            try:
                parser = CronParser(task.cron_expression)
                next_time = parser.next_run_after()
                new_task = Task.create(
                    task_name=task.task_name,
                    payload=task.payload,
                    priority=task.priority,
                    max_retries=task.max_retries,
                    delay_seconds=max(1, int(next_time.timestamp() - time.time())),
                    cron_expression=task.cron_expression,
                )
                self.enqueue_task(new_task)
                logger.info("Cron task %s rescheduled, next run at %s", task.task_id, next_time)
            except Exception as e:
                logger.exception("Failed to reschedule cron task %s: %s", task.task_id, e)

    def fail_task(self, task: Task, error: str) -> None:
        task.retry_count += 1
        task.error_message = error
        self._handle_retry_or_dead_letter(task, error)

    def _handle_retry_or_dead_letter(self, task: Task, error: str) -> None:
        self.redis.zrem(self._k(KEY_PROCESSING), task.task_id)

        if task.retry_count >= task.max_retries:
            task.status = TaskStatus.DEAD_LETTER
            task.finished_at = time.time()
            self.save_task(task)
            self.redis.lpush(self._k(KEY_DEAD_LETTER), task.task_id)
            final_result = TaskResult(
                task_id=task.task_id,
                success=False,
                error=error,
                retry_count=task.retry_count,
            )
            self.redis.set(self._k(KEY_RESULT.format(task_id=task.task_id)), final_result.to_json())
            self.redis.publish(self._k(KEY_RESULT_CHANNEL), final_result.to_json())
            logger.error("Task %s moved to dead letter queue after %d retries. Error: %s",
                         task.task_id, task.retry_count, error)
            return

        backoff_seconds = min(60 * 60, 2 ** task.retry_count)
        task.status = TaskStatus.DELAYED
        task.scheduled_at = time.time() + backoff_seconds
        task.worker_id = None
        task.started_at = 0.0
        self.save_task(task)
        self.redis.zadd(self._k(KEY_DELAYED), {task.task_id: task.scheduled_at})
        logger.info("Task %s scheduled for retry %d/%d in %d seconds",
                    task.task_id, task.retry_count, task.max_retries, backoff_seconds)

    def get_result(self, task_id: str, timeout: float = 0) -> Optional[TaskResult]:
        if timeout > 0:
            start = time.time()
            pubsub = self.redis.pubsub()
            pubsub.subscribe(self._k(KEY_RESULT_CHANNEL))
            try:
                while time.time() - start < timeout:
                    message = pubsub.get_message(timeout=min(0.5, timeout - (time.time() - start)))
                    if message and message["type"] == "message":
                        try:
                            r = TaskResult.from_json(message["data"])
                            if r.task_id == task_id:
                                return r
                        except Exception:
                            pass
                    existing = self.redis.get(self._k(KEY_RESULT.format(task_id=task_id)))
                    if existing:
                        return TaskResult.from_json(existing)
            finally:
                pubsub.unsubscribe()
        else:
            existing = self.redis.get(self._k(KEY_RESULT.format(task_id=task_id)))
            if existing:
                return TaskResult.from_json(existing)
        return None

    def list_dead_letter_tasks(self, start: int = 0, end: int = -1) -> List[Task]:
        task_ids = self.redis.lrange(self._k(KEY_DEAD_LETTER), start, end)
        tasks = []
        for tid in task_ids:
            t = self.get_task(tid)
            if t:
                tasks.append(t)
        return tasks

    def requeue_dead_letter_task(self, task_id: str) -> bool:
        removed = self.redis.lrem(self._k(KEY_DEAD_LETTER), 0, task_id)
        if removed == 0:
            return False
        task = self.get_task(task_id)
        if task is None:
            return False
        task.retry_count = 0
        task.status = TaskStatus.READY
        task.error_message = ""
        self.save_task(task)
        queue_key = self._k(KEY_QUEUE.format(priority=int(task.priority)))
        self.redis.lpush(queue_key, task_id)
        logger.info("Dead letter task %s re-queued", task_id)
        return True

    def _cron_loop(self) -> None:
        logger.info("Cron scheduler started")
        while not self._stop_event.is_set():
            self._stop_event.wait(5.0)
