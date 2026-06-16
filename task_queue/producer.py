import time
import logging
from typing import Any, Dict, Optional

from .broker import Broker
from .models import Task, TaskPriority, TaskResult, TaskStatus
from .cron import CronParser

logger = logging.getLogger(__name__)


class Producer:
    def __init__(self, broker: Broker):
        self.broker = broker
        self.broker.start_background_threads()

    def send(
        self,
        task_name: str,
        payload: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        delay_seconds: int = 0,
        idempotency_key: Optional[str] = None,
    ) -> Task:
        task = Task.create(
            task_name=task_name,
            payload=payload or {},
            priority=priority,
            max_retries=max_retries,
            delay_seconds=delay_seconds,
            idempotency_key=idempotency_key,
        )
        self.broker.enqueue_task(task)
        logger.info("Sent task %s (%s)", task.task_id, task_name)
        return task

    def send_cron(
        self,
        task_name: str,
        cron_expression: str,
        payload: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        idempotency_key: Optional[str] = None,
    ) -> Task:
        parser = CronParser(cron_expression)
        next_run = parser.next_run_after()
        delay = max(1, int(next_run.timestamp() - time.time()))
        task = Task.create(
            task_name=task_name,
            payload=payload or {},
            priority=priority,
            max_retries=max_retries,
            delay_seconds=delay,
            idempotency_key=idempotency_key,
            cron_expression=cron_expression,
        )
        self.broker.enqueue_task(task)
        logger.info("Scheduled cron task %s (%s) with expression %s, next run at %s",
                    task.task_id, task_name, cron_expression, next_run)
        return task

    def wait_for_result(self, task_id: str, timeout: float = 60.0) -> Optional[TaskResult]:
        return self.broker.get_result(task_id, timeout=timeout)

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        return self.broker.get_result(task_id)
