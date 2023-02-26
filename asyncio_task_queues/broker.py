from abc import ABC, abstractmethod

from asyncio_task_queues.event import EventSystem
from asyncio_task_queues.job import Job
from asyncio_task_queues.task import Task
from asyncio_task_queues.types import TYPE_CHECKING, Optional, Set

if TYPE_CHECKING:
    from asyncio_task_queues.app import App


class Broker(ABC):
    app_name: str
    events: EventSystem

    def __init__(self):
        self.app_name = "unset"
        self.events = EventSystem()

    def bind(self, app: "App"):
        self.app_name = app.name
        self.events = app.events

    async def initialize(self):
        pass

    @abstractmethod
    async def get_jobs(self, *ids: str) -> list[Optional[Job]]:
        ...

    @abstractmethod
    async def enqueue_task(self, task: Task) -> Job:
        ...

    @abstractmethod
    async def update_job(self, job: Job):
        ...

    @abstractmethod
    async def request_job(self, worker_name: str, queues: Set[str]) -> Optional[Job]:
        ...
