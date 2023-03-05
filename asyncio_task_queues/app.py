import asyncio
from enum import Enum
from uuid import uuid4

from asyncio_task_queues.backend import Backend
from asyncio_task_queues.broker import Broker
from asyncio_task_queues.chain import Chain
from asyncio_task_queues.event import System as EventSystem
from asyncio_task_queues.group import Group
from asyncio_task_queues.job import Job
from asyncio_task_queues.schedule import Schedule
from asyncio_task_queues.signature import PS as SignaturePS
from asyncio_task_queues.signature import RV as SignatureRV
from asyncio_task_queues.signature import Signature
from asyncio_task_queues.task import ScheduledTask, Task
from asyncio_task_queues.types import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
)
from asyncio_task_queues.worker import Worker

ACallable = TypeVar("ACallable", bound=Callable)
AQueue = TypeVar("AQueue", bound=Enum)


class App(Generic[AQueue]):
    backend: Backend
    broker: Broker
    events: EventSystem
    name: str
    queue_default: AQueue
    scheduled_tasks: Dict[str, ScheduledTask]
    worker_cls: Type[Worker]

    def __init__(
        self,
        name: str,
        *,
        backend: Backend,
        broker: Broker,
        queue_default: AQueue,
        worker_cls: Optional[Type[Worker]] = None,
    ):
        worker_cls = worker_cls or Worker

        self.backend = backend
        self.broker = broker
        self.events = EventSystem()
        self.name = name
        self.queue_default = queue_default
        self.scheduled_tasks = {}
        self.worker_cls = worker_cls

        self.backend.bind(self)
        self.broker.bind(self)

    async def initialize(self):
        await asyncio.gather(self.backend.initialize(), self.broker.initialize())

    def create_task(
        self,
        callable: Callable[SignaturePS, SignatureRV],
        *,
        args: Optional[Tuple] = None,
        id: Optional[str] = None,
        kwargs: Optional[Dict] = None,
        queue: Optional[AQueue] = None,
    ) -> "Task[SignaturePS, SignatureRV, AQueue]":
        id = id or str(uuid4())
        signature = Signature.from_function(callable, args=args, kwargs=kwargs)
        queue = queue or self.queue_default

        return Task(id=id, signature=signature, queue=queue)

    def create_chain(self, *tasks: Task) -> "Chain[AQueue]":
        return Chain(id=str(uuid4()), queue=self.queue_default, tasks=list(tasks))

    def create_group(self, *tasks: Task) -> "Group[AQueue]":
        return Group(id=str(uuid4()), queue=self.queue_default, tasks=list(tasks))

    def register_scheduled_task(self, scheduled_task: ScheduledTask):
        if scheduled_task.id in self.scheduled_tasks:
            raise ValueError(
                f"scheduled task id already registered: {scheduled_task.id}"
            )
        self.scheduled_tasks[scheduled_task.id] = scheduled_task

    def create_scheduled_task(
        self,
        callable: Callable[SignaturePS, SignatureRV],
        schedule: Schedule,
        *,
        args: Optional[Tuple] = None,
        id: str,
        kwargs: Optional[Dict] = None,
        queue: Optional[AQueue] = None,
        unique: Optional[bool] = None,
    ) -> "ScheduledTask[SignaturePS, SignatureRV, AQueue]":
        queue = queue or self.queue_default
        unique = unique if unique is not None else False

        signature = Signature.from_function(callable, args=args, kwargs=kwargs)
        scheduled_task = ScheduledTask(
            id=id,
            queue=queue,
            schedule=schedule,
            signature=signature,
            unique=unique,
        )
        return scheduled_task

    def scheduled_task(
        self,
        schedule: Schedule,
        *,
        args: Optional[Tuple] = None,
        id: str,
        kwargs: Optional[Dict] = None,
        queue: Optional[AQueue] = None,
        unique: Optional[bool] = None,
    ) -> Callable[[ACallable], ACallable]:
        def inner(callable: ACallable) -> ACallable:
            scheduled_task = self.create_scheduled_task(
                callable=callable,
                schedule=schedule,
                args=args,
                id=id,
                kwargs=kwargs,
                queue=queue,
                unique=unique,
            )
            self.register_scheduled_task(scheduled_task)
            return callable

        return inner

    async def run_worker(
        self,
        name: str,
        *,
        concurrency: Optional[int] = None,
        poll_rate: Optional[float] = None,
        queues: Optional[Set[AQueue]] = None,
    ):
        queues = queues or {self.queue_default}

        worker = self.worker_cls(
            broker=self.broker,
            concurrency=concurrency,
            events=self.events,
            name=name,
            poll_rate=poll_rate,
            queues={q.value for q in queues},
            scheduled_tasks=list(self.scheduled_tasks.values()),
        )
        await worker.run()
