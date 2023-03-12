from asyncio_task_queues.context import Context, InjectedContext
from asyncio_task_queues.signature import Signature
from asyncio_task_queues.status import Status, StatusException
from asyncio_task_queues.task import Task, TaskQueue
from asyncio_task_queues.types import Any, Generic, List


async def group_function(*, tasks: List[Task], context: Context = InjectedContext):
    jobs = []
    for task in tasks:
        (job,) = await context.broker.get_backend().get_jobs(task.id)
        if not job:
            job = await context.broker.enqueue_task(task)
        jobs.append(job)

    statuses = {s: 0 for s in Status.__members__.values()}
    for job in jobs:
        statuses[job.status] += 1

    if statuses[Status.Successful] == len(tasks):
        return

    if not statuses[Status.Cancelled] and not statuses[Status.Failed]:
        raise StatusException.deferred()

    for job in jobs:
        await context.broker.cancel_job(job)
    raise RuntimeError(f"child task not successful")


class Group(Task[Any, Any, TaskQueue], Generic[TaskQueue]):
    def __init__(self, id: str, queue: TaskQueue, tasks: List[Task]):
        signature = Signature.from_function(group_function).with_args(tasks=tasks)
        super().__init__(id=id, queue=queue, signature=signature)
