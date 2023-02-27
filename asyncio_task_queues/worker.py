import asyncio
import datetime
from signal import SIGINT, SIGTERM, Signals

from asyncio_task_queues.broker import Broker
from asyncio_task_queues.event import (
    EventSystem,
    JobStatusChangeEvent,
    WorkerShutdownEvent,
    WorkerStartEvent,
)
from asyncio_task_queues.job import Job, Status
from asyncio_task_queues.middleware import Middleware
from asyncio_task_queues.task import ScheduledTask
from asyncio_task_queues.types import Any, Dict, List, Optional, Set, TypeVar


class Worker:
    broker: Broker
    concurrency: int
    events: EventSystem
    middleware: List[Middleware]
    name: str
    poll_rate: float
    queues: Set[str]
    scheduled_tasks: List[ScheduledTask]
    stopped: bool
    tasks_jobs: Dict[str, asyncio.Task]
    tasks_job_monitors: Set[asyncio.Task]
    tasks_schedulers: Set[asyncio.Task]

    def __init__(
        self,
        *,
        broker: Broker,
        concurrency: Optional[int] = None,
        events: Optional[EventSystem] = None,
        middleware: Optional[List[Middleware]] = None,
        name: str,
        poll_rate: Optional[float] = None,
        queues: Set[str],
        scheduled_tasks: Optional[List[ScheduledTask]],
    ):
        concurrency = concurrency or 1
        events = events or EventSystem()
        middleware = middleware or []
        poll_rate = poll_rate or 0.5
        scheduled_tasks = scheduled_tasks or []

        self.broker = broker
        self.concurrency = concurrency
        self.events = events
        self.middleware = middleware
        self.name = name
        self.poll_rate = poll_rate
        self.queues = queues
        self.scheduled_tasks = scheduled_tasks
        self.stopped = False
        self.tasks_jobs = {}
        self.tasks_job_monitors = set()
        self.tasks_schedulers = set()

    async def handle_signal_async(self, signal: Signals):
        if signal in [SIGINT, SIGTERM]:
            await self.stop()

    def handle_signal(self, signal: Signals):
        loop = asyncio.get_running_loop()
        loop.create_task(self.handle_signal_async(signal))

    async def install_signal_handlers(self):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(SIGINT, lambda: self.handle_signal(SIGINT))
        loop.add_signal_handler(SIGTERM, lambda: self.handle_signal(SIGTERM))

    async def start(self):
        self.stopped = False
        await self.install_signal_handlers()
        await self.start_schedulers()
        event = WorkerStartEvent(worker_name=self.name)
        await self.events.worker_start.publish(event)

    async def loop(self):
        while not self.stopped:
            await self.request_new_jobs()
            await asyncio.sleep(self.poll_rate)

    async def shutdown(self):
        event = WorkerShutdownEvent(worker_name=self.name)
        await self.events.worker_shutdown.publish(event)

        cancelled: Set[asyncio.Task] = set()
        all_tasks = (
            set(self.tasks_jobs.values())
            .union(self.tasks_job_monitors)
            .union(self.tasks_schedulers)
        )
        for task in all_tasks:
            if task.cancel():
                cancelled.add(task)
        await asyncio.gather(*cancelled)

    async def stop(self):
        self.stopped = True

    async def main(self):
        await self.start()
        try:
            await self.loop()
        finally:
            await self.shutdown()

    async def request_new_job(self) -> Optional[Job]:
        job = await self.broker.request_job(self.name, self.queues)
        if not job:
            return
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.run_job(job))
        self.tasks_jobs[job.id] = task
        task.add_done_callback(lambda t: pop_value(self.tasks_jobs, t))

    async def request_new_jobs(self):
        active_jobs = len(self.tasks_jobs)
        for _ in range(active_jobs, self.concurrency):
            if not await self.request_new_job():
                break

    async def run_job(self, job: Job):
        async def update_job():
            update_coro = self.broker.update_job(job)
            try:
                await asyncio.shield(update_coro)
            except asyncio.CancelledError as e:
                raise e

        try:
            job.time_started = datetime.datetime.now(tz=datetime.timezone.utc)
            job.status = Status.InProgress
            await update_job()

            await self.start_job_monitor(job)
            job.return_value = await job.signature()
            job.status = Status.Successful
        except asyncio.CancelledError as e:
            return
        except Exception as e:
            job.return_value = e
            job.status = Status.Failed
        finally:
            job.time_completed = datetime.datetime.now(tz=datetime.timezone.utc)
            await update_job()

    async def run_job_monitor(self, job: Job):
        job_id = job.id

        async def cancel_job():
            task = self.tasks_jobs.get(job_id)
            if task and task.cancel():
                await task

        while True:
            (curr_job,) = await self.broker.get_jobs(job_id)
            # NOTE: edge case - job record expired, but still in progress
            if not curr_job:
                await cancel_job()
                break
            if curr_job.status == Status.Cancelled:
                await cancel_job()
            if curr_job.status.is_complete():
                break
            await asyncio.sleep(self.poll_rate)

    async def start_job_monitor(self, job: Job):
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.run_job_monitor(job))
        self.tasks_job_monitors.add(task)
        task.add_done_callback(lambda t: self.tasks_job_monitors.remove(t))

    async def run_scheduler(self, scheduled_task: ScheduledTask):
        dt_prev = datetime.datetime.now(tz=datetime.timezone.utc)
        while not self.stopped:
            dt_next = scheduled_task.schedule.next_datetime(dt_prev)
            sleep_time = (dt_next - dt_prev).total_seconds()
            await asyncio.sleep(sleep_time)
            await self.broker.enqueue_task(scheduled_task.create_task())
            dt_prev = dt_next

    async def start_scheduler(self, scheduled_task: ScheduledTask):
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.run_scheduler(scheduled_task))
        self.tasks_schedulers.add(task)
        task.add_done_callback(lambda t: self.tasks_schedulers.remove(t))

    async def start_schedulers(self):
        await asyncio.gather(*map(self.start_scheduler, self.scheduled_tasks))

    async def run(self):
        try:
            await self.main()
        except asyncio.CancelledError:
            pass


Value = TypeVar("Value")


def pop_value(data: dict[str, Value], v: Value):
    for key, value in dict(data).items():
        if value == v:
            data.pop(key, None)
