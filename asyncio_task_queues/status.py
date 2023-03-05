from enum import Enum
from typing import Any, Optional


class Status(str, Enum):
    Queued = "queued"
    InProgress = "in-progress"
    Cancelling = "cancelling"
    Cancelled = "cancelled"
    Deferred = "deferred"
    Failed = "failed"
    Successful = "success"

    def is_complete(self) -> bool:
        return self in {self.Cancelled, self.Deferred, self.Failed, self.Successful}


class StatusException(Exception):
    return_value: Any
    status: Status

    def __init__(self, status: Status, return_value: Optional[Any] = None):
        return_value = return_value or None

        self.return_value = return_value
        self.status = status

        super().__init__(str(self.status.value))

    @classmethod
    def cancelled(cls) -> "StatusException":
        return StatusException(status=Status.Cancelled)

    @classmethod
    def deferred(cls) -> "StatusException":
        return StatusException(status=Status.Deferred)

    @classmethod
    def failed(cls, return_value: Exception) -> "StatusException":
        return StatusException(return_value=return_value, status=Status.Failed)

    @classmethod
    def successful(cls, return_value: Any) -> "StatusException":
        return StatusException(return_value=return_value, status=Status.Successful)
