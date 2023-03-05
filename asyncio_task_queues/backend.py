import datetime
from abc import ABC, abstractmethod

from asyncio_task_queues.event import JobSaveEvent
from asyncio_task_queues.event import System as EventSystem
from asyncio_task_queues.job import Job
from asyncio_task_queues.types import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from asyncio_task_queues.app import App


class Backend(ABC):
    _app_name: Optional[str]
    events: EventSystem

    def __init__(
        self,
        *,
        app_name: Optional[str] = None,
    ):
        self._app_name = app_name
        self.events = EventSystem()

    def get_app_name(self) -> str:
        if not self._app_name:
            raise ValueError("app name")
        return self._app_name

    def bind(self, app: "App"):
        self._app_name = app.name
        self.events = app.events

    async def initialize(self):
        pass

    @abstractmethod
    async def _save_job(self, job: Job):
        ...

    async def save_job(self, job: Job):
        await self._save_job(job)
        await self.events.publish(
            JobSaveEvent(job_id=job.id, job_status=job.status.value)
        )

    @abstractmethod
    async def get_jobs(self, *ids: str) -> List[Optional[Job]]:
        ...

    @abstractmethod
    async def list_jobs(self) -> List[Job]:
        ...
