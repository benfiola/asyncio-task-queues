import asyncio
import datetime

from asyncio_task_queues.types import Dict, Protocol, Set, Type, TypeVar


class Event:
    event_id: str
    event_time: datetime.datetime

    def __init__(self, *, event_id: str):
        self.event_id = event_id
        self.event_time = datetime.datetime.utcnow()


class JobSaveEvent(Event):
    job_id: str
    job_status: str

    def __init__(self, job_id: str, job_status: str):
        super().__init__(event_id="job-save")
        self.job_id = job_id
        self.job_status = job_status


class WorkerJobStartEvent(Event):
    job_id: str
    worker_name: str

    def __init__(self, job_id: str, worker_name: str):
        super().__init__(event_id="worker-job-start")
        self.job_id = job_id
        self.worker_name = worker_name


class WorkerJobFinishEvent(Event):
    job_id: str
    job_status: str
    worker_name: str

    def __init__(self, job_id: str, job_status: str, worker_name: str):
        super().__init__(event_id="worker-job-finish")
        self.job_id = job_id
        self.job_status = job_status
        self.worker_name = worker_name


class WorkerStartEvent(Event):
    worker_name: str

    def __init__(self, worker_name: str):
        super().__init__(event_id="worker-start")
        self.worker_name = worker_name


class WorkerStopEvent(Event):
    worker_name: str

    def __init__(self, worker_name: str):
        super().__init__(event_id="worker-stop")
        self.worker_name = worker_name


CallbackEvent = TypeVar("CallbackEvent", bound=Event, contravariant=True)


class Callback(Protocol[CallbackEvent]):
    async def __call__(self, event: CallbackEvent):
        ...


SystemEvent = TypeVar("SystemEvent", bound=Event)


class System:
    subscribers: Dict[Type[Event], Set[Callback]]

    def __init__(self):
        self.subscribers = {}

    def subscribe(self, event_cls: Type[SystemEvent], callback: Callback[SystemEvent]):
        callbacks = self.subscribers.setdefault(event_cls, set())
        callbacks.add(callback)

    async def publish(self, event: Event):
        callbacks = self.subscribers.get(event.__class__, set())
        tasks = []
        for callback in callbacks:
            tasks.append(callback(event))
        await asyncio.gather(*tasks)
