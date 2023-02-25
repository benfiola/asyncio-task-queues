import datetime
from enum import Enum

from asyncio_task_queues.signature import Signature
from asyncio_task_queues.types import Any, Optional


class Status(str, Enum):
    Queued = "queued"
    InProgress = "in-progress"
    Successful = "success"
    Failed = "failed"
    Cancelled = "cancelled"


class Job:
    id: str
    signature: Signature
    queue: str
    status: Status
    return_value: Any
    time_completed: datetime.datetime
    time_enqueued: Optional[datetime.datetime]
    time_started: Optional[datetime.datetime]
