import asyncio

from asyncio_task_queues.types import Dict, Generic, Literal, Protocol, TypeVar, cast


class Event:
    type: str = cast(str, None)

    def __init__(self):
        if self.__class__.type is None:
            raise RuntimeError(f"class does not have type set: {self.__class__}")
        self.type = self.__class__.type


ECEvent = TypeVar("ECEvent", bound=Event, contravariant=True)


class EventCallback(Protocol[ECEvent]):
    async def __call__(self, event: ECEvent):
        ...


ESEvent = TypeVar("ESEvent", bound=Event)


class EventSource(Generic[ESEvent]):
    subscribers: Dict[str, EventCallback[ESEvent]]

    def __init__(self):
        self.subscribers = {}

    def get_callback_id(self, callback: EventCallback[ESEvent]):
        module = getattr(callback, "__module__")
        name = getattr(callback, "__name__")
        if not all([module, name]):
            raise ValueError(f"unable to get id for callback: {callback}")

        return f"{module}:{name}"

    def subscribe(self, callback: EventCallback[ESEvent]):
        id = self.get_callback_id(callback)
        self.subscribers[id] = callback

    def unsubscribe(self, callback: EventCallback[ESEvent]):
        id = self.get_callback_id(callback)
        self.subscribers.pop(id)

    async def publish(self, event: ESEvent):
        tasks = []
        for callback in self.subscribers.values():
            tasks.append(callback(event))
        await asyncio.gather(*tasks)


class JobStatusChangeEvent(Event):
    type: Literal["job-status-change"] = "job-status-change"

    job_id: str
    status: str

    def __init__(self, *, job_id: str, status: str):
        self.job_id = job_id
        self.status = status


class WorkerShutdownEvent(Event):
    type: Literal["worker-shutdown"] = "worker-shutdown"

    worker_name: str

    def __init__(self, *, worker_name: str):
        super().__init__()
        self.worker_name = worker_name


class WorkerStartEvent(Event):
    type: Literal["worker-start"] = "worker-start"

    worker_name: str

    def __init__(self, *, worker_name: str):
        super().__init__()
        self.worker_name = worker_name


class EventSystem:
    job_status_change: EventSource[JobStatusChangeEvent]
    worker_shutdown: EventSource[WorkerShutdownEvent]
    worker_start: EventSource[WorkerStartEvent]

    def __init__(self):
        self.job_status_change = EventSource()
        self.worker_shutdown = EventSource()
        self.worker_start = EventSource()
