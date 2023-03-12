from asyncio_task_queues.context import Context, InjectedContext
from asyncio_task_queues.signature import Signature
from asyncio_task_queues.status import Status, StatusException
from asyncio_task_queues.task import Task, TaskQueue
from asyncio_task_queues.types import Any, Generic, List


async def chain_function(*, tasks: List[Task], context: Context = InjectedContext):
    for task in tasks:
        (job,) = await context.broker.get_backend().get_jobs(task.id)
        if not job:
            job = await context.broker.enqueue_task(task)
        if not job.status.is_complete():
            raise StatusException.deferred()
        if job.status != Status.Successful:
            raise RuntimeError(f"child task not successful")


class Chain(Task[Any, Any, TaskQueue], Generic[TaskQueue]):
    def __init__(self, id: str, queue: TaskQueue, tasks: List[Task]):
        signature = Signature.from_function(chain_function).with_args(tasks=tasks)
        super().__init__(id=id, queue=queue, signature=signature)
