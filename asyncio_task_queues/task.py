from enum import Enum

from asyncio_task_queues.schedule import Schedule
from asyncio_task_queues.signature import Signature, SignaturePS, SignatureRV
from asyncio_task_queues.types import Generic, TypeVar

TaskQueue = TypeVar("TaskQueue")


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
        self.id = id
        self.queue = self.convert_queue(queue)
        self.signature = signature

    @classmethod
    def convert_queue(cls, queue: TaskQueue) -> str:
        if isinstance(queue, Enum):
            queue = queue.value
        if not isinstance(queue, str):
            raise NotImplementedError(type(queue))
        return queue


class ScheduledTask(
    Task[SignaturePS, SignatureRV, TaskQueue],
    Generic[SignaturePS, SignatureRV, TaskQueue],
):
    schedule: Schedule
