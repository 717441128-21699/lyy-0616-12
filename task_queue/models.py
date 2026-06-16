import json
import uuid
import time
from enum import IntEnum
from dataclasses import dataclass, field, asdict
from typing import Any, Optional, Dict


class TaskStatus(IntEnum):
    PENDING = 0
    DELAYED = 1
    READY = 2
    PROCESSING = 3
    SUCCESS = 4
    FAILED = 5
    DEAD_LETTER = 6


class TaskPriority(IntEnum):
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Task:
    task_id: str
    task_name: str
    payload: Dict[str, Any]
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    scheduled_at: float = 0.0
    started_at: float = 0.0
    finished_at: float = 0.0
    visibility_timeout: int = 300
    worker_id: Optional[str] = None
    error_message: str = ""
    idempotency_key: Optional[str] = None
    cron_expression: Optional[str] = None

    @classmethod
    def create(
        cls,
        task_name: str,
        payload: Dict[str, Any],
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        delay_seconds: int = 0,
        idempotency_key: Optional[str] = None,
        cron_expression: Optional[str] = None,
    ) -> "Task":
        now = time.time()
        return cls(
            task_id=str(uuid.uuid4()),
            task_name=task_name,
            payload=payload,
            priority=priority,
            status=TaskStatus.DELAYED if delay_seconds > 0 else TaskStatus.READY,
            max_retries=max_retries,
            created_at=now,
            updated_at=now,
            scheduled_at=now + delay_seconds,
            idempotency_key=idempotency_key,
            cron_expression=cron_expression,
        )

    def to_json(self) -> str:
        data = asdict(self)
        data["priority"] = int(self.priority)
        data["status"] = int(self.status)
        return json.dumps(data, ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "Task":
        data = json.loads(s)
        data["priority"] = TaskPriority(data["priority"])
        data["status"] = TaskStatus(data["status"])
        return cls(**data)

    def touch(self) -> None:
        self.updated_at = time.time()


@dataclass
class TaskResult:
    task_id: str
    success: bool
    result: Any = None
    error: str = ""
    executed_at: float = field(default_factory=time.time)
    retry_count: int = 0

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, s: str) -> "TaskResult":
        data = json.loads(s)
        return cls(**data)
