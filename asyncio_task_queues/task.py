from enum import Enum
from uuid import uuid4

from asyncio_task_queues.schedule import Schedule
from asyncio_task_queues.signature import PS as SignaturePS
from asyncio_task_queues.signature import RV as SignatureRV
from asyncio_task_queues.signature import Signature
from asyncio_task_queues.types import Any, Generic, Optional, TypeVar, Union, cast

TaskQueue = TypeVar("TaskQueue")


def convert_queue(queue: Any) -> str:
    if isinstance(queue, Enum):
        queue = queue.value
    if not isinstance(queue, str):
        raise NotImplementedError(type(queue))
    return queue


class Task(Generic[SignaturePS, SignatureRV, TaskQueue]):
    id: str
    queue: str
    signature: Signature[SignaturePS, SignatureRV]

    def __init__(
        self,
        *,
        id: str,
        queue: TaskQueue,
        signature: Signature[SignaturePS, SignatureRV],
    ):
        self.signature = signature
        self.with_options(queue=queue, id=id)

    def with_args(
        self, *args: SignaturePS.args, **kwargs: SignaturePS.kwargs
    ) -> "Task[SignaturePS, SignatureRV, TaskQueue]":
        self.signature.with_args(*args, **kwargs)
        return self

    def with_options(
        self, *, queue: Optional[TaskQueue] = None, id: Optional[str] = None
    ) -> "Task[SignaturePS, SignatureRV, TaskQueue]":
        if queue is not None:
            self.queue = convert_queue(queue)
        if id is not None:
            self.id = id
        return self


class ScheduledTask(
    Generic[SignaturePS, SignatureRV, TaskQueue],
):
    id: str
    queue: str
    schedule: Schedule
    signature: Signature[SignaturePS, SignatureRV]
    unique: bool

    def __init__(
        self,
        *,
        id: str,
        queue: TaskQueue,
        schedule: Schedule,
        signature: Signature[SignaturePS, SignatureRV],
        unique: bool,
    ):
        self.schedule = schedule
        self.signature = signature
        self.with_options(id=id, queue=queue, unique=unique)

    def with_args(
        self, *args: SignaturePS.args, **kwargs: SignaturePS.kwargs
    ) -> "ScheduledTask[SignaturePS, SignatureRV, TaskQueue]":
        self.signature.with_args(*args, **kwargs)
        return self

    def with_options(
        self,
        *,
        id: Optional[str] = None,
        queue: Optional[TaskQueue] = None,
        unique: Optional[bool] = None,
    ):
        if id is not None:
            self.id = id
        if queue is not None:
            self.queue = convert_queue(queue)
        if unique is not None:
            self.unique = unique
        return self

    def create_task(self) -> Task[SignaturePS, SignatureRV, TaskQueue]:
        id = self.id
        if self.unique:
            id = f"{self.id}:{uuid4()}"
        queue = cast(TaskQueue, self.queue)
        return Task(id=id, queue=queue, signature=self.signature)
