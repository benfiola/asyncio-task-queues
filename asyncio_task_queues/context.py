from asyncio_task_queues.types import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from asyncio_task_queues.broker import Broker
    from asyncio_task_queues.job import Job


class Context:
    broker: "Broker"
    job: "Job"

    def __init__(self, broker: "Broker", job: "Job"):
        self.broker = broker
        self.job = job


InjectedContext = cast(Context, None)
