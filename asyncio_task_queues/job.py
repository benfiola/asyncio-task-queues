import datetime
from enum import Enum

from asyncio_task_queues.signature import Signature
from asyncio_task_queues.status import Status
from asyncio_task_queues.types import Any, Optional


class Job:
    id: str
    signature: Signature
    queue: str
    return_value: Any
    status: Status
    time_completed: Optional[datetime.datetime]
    time_enqueued: datetime.datetime
    time_started: Optional[datetime.datetime]

    def __init__(
        self,
        *,
        id: str,
        signature: Signature,
        queue: str,
        return_value: Any,
        status: Status,
        time_completed: Optional[datetime.datetime],
        time_enqueued: datetime.datetime,
        time_started: Optional[datetime.datetime],
    ):
        self.id = id
        self.signature = signature
        self.queue = queue
        self.return_value = return_value
        self.status = status
        self.time_completed = time_completed
        self.time_enqueued = time_enqueued
        self.time_started = time_started
