from abc import ABC, abstractmethod

from asyncio_task_queues.backend import Backend
from asyncio_task_queues.event import System as EventSystem
from asyncio_task_queues.job import Job
from asyncio_task_queues.task import Task
from asyncio_task_queues.types import TYPE_CHECKING, Optional, Set

if TYPE_CHECKING:
    from asyncio_task_queues.app import App
    from asyncio_task_queues.worker import Worker


class Broker(ABC):
    _app_name: Optional[str]
    _backend: Optional[Backend]
    events: EventSystem

    def __init__(
        self, *, app_name: Optional[str] = None, backend: Optional[Backend] = None
    ):
        self._app_name = app_name
        self._backend = backend
        self.events = EventSystem()

    def get_app_name(self) -> str:
        if self._app_name is None:
            raise ValueError("app name")
        return self._app_name

    def get_backend(self) -> Backend:
        if self._backend is None:
            raise ValueError("backend")
        return self._backend

    def bind(self, app: "App"):
        self._app_name = app.name
        self._backend = app.backend
        self.events = app.events

    async def initialize(self):
        pass

    @abstractmethod
    async def cancel_job(self, job: Job):
        ...

    @abstractmethod
    async def enqueue_task(self, task: Task) -> Job:
        ...

    @abstractmethod
    async def worker_update_job(self, worker_id: str, job: Job):
        ...

    @abstractmethod
    async def worker_update(self, worker_id: str):
        ...

    @abstractmethod
    async def worker_request_job(self, worker: str, queues: Set[str]) -> Optional[Job]:
        ...
