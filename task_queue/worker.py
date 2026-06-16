import time
import uuid
import logging
import threading
import traceback
from typing import Dict, Callable, Any, Optional

from .broker import Broker
from .models import Task, TaskResult, TaskStatus

logger = logging.getLogger(__name__)

TaskHandler = Callable[[Dict[str, Any]], Any]


class Worker:
    def __init__(self, broker: Broker, worker_id: Optional[str] = None,
                 visibility_timeout: int = 300, heartbeat_interval: int = 60):
        self.broker = broker
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.visibility_timeout = visibility_timeout
        self.heartbeat_interval = heartbeat_interval
        self._handlers: Dict[str, TaskHandler] = {}
        self._stop_event = threading.Event()
        self._active_tasks: Dict[str, threading.Event] = {}
        self._heartbeat_thread: Optional[threading.Thread] = None
        self.broker.start_background_threads()

    def register(self, task_name: str, handler: TaskHandler) -> None:
        self._handlers[task_name] = handler
        logger.info("Worker %s registered handler for task: %s", self.worker_id, task_name)

    def unregister(self, task_name: str) -> None:
        self._handlers.pop(task_name, None)

    def start(self) -> None:
        logger.info("Worker %s starting...", self.worker_id)
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()
        try:
            self._run_loop()
        finally:
            self._stop_event.set()
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                self._heartbeat_thread.join(timeout=5)
            logger.info("Worker %s stopped", self.worker_id)

    def stop(self) -> None:
        self._stop_event.set()

    def _heartbeat_loop(self) -> None:
        logger.info("Heartbeat thread started for worker %s", self.worker_id)
        while not self._stop_event.is_set():
            for task_id in list(self._active_tasks.keys()):
                renewed = self.broker.renew_lease(task_id, self.worker_id, self.visibility_timeout)
                if not renewed:
                    logger.warning("Could not renew lease for task %s, worker %s", task_id, self.worker_id)
                    self._active_tasks.pop(task_id, None)
            self._stop_event.wait(self.heartbeat_interval)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                task = self.broker.fetch_task(self.worker_id, self.visibility_timeout)
                if task is None:
                    self._stop_event.wait(0.5)
                    continue
                self._execute_task(task)
            except Exception as e:
                logger.exception("Worker loop error: %s", e)
                self._stop_event.wait(1.0)

    def _execute_task(self, task: Task) -> None:
        done_event = threading.Event()
        self._active_tasks[task.task_id] = done_event
        logger.info("Worker %s executing task %s (%s)", self.worker_id, task.task_id, task.task_name)
        try:
            handler = self._handlers.get(task.task_name)
            if handler is None:
                raise ValueError(f"No handler registered for task: {task.task_name}")

            result_value = handler(task.payload)
            task_result = TaskResult(
                task_id=task.task_id,
                success=True,
                result=result_value,
                retry_count=task.retry_count,
            )
            self.broker.complete_task(task, task_result)
            logger.info("Task %s completed successfully by worker %s", task.task_id, self.worker_id)

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
            logger.error("Task %s failed on worker %s: %s", task.task_id, self.worker_id, error_msg)
            self.broker.fail_task(task, error_msg)
        finally:
            self._active_tasks.pop(task.task_id, None)
            done_event.set()
