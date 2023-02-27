import asyncio
import contextlib
import datetime
import random
from uuid import uuid4

import redis.asyncio
import redis.asyncio.client

from asyncio_task_queues.broker import Broker as BaseBroker
from asyncio_task_queues.event import JobStatusChangeEvent
from asyncio_task_queues.job import Job, Status
from asyncio_task_queues.serializer import PickleSerializer, Serializer
from asyncio_task_queues.task import Task
from asyncio_task_queues.types import AsyncGenerator, Optional, Set, Type, cast


class Broker(BaseBroker):
    expiry_job_heartbeat: datetime.timedelta
    expiry_lock: datetime.timedelta
    expiry_worker_heartbeat: datetime.timedelta
    redis_cls: Type[redis.asyncio.Redis]
    redis_url: str
    serializer_cls: Type[Serializer]
    _redis: Optional[redis.asyncio.Redis]

    def __init__(
        self,
        redis_url: str,
        *,
        expiry_job_heartbeat: Optional[datetime.timedelta] = None,
        expiry_lock: Optional[datetime.timedelta] = None,
        expiry_worker_heartbeat: Optional[datetime.timedelta] = None,
        redis_cls: Optional[Type[redis.asyncio.Redis]] = None,
        serializer_cls: Optional[Type[Serializer]] = None,
    ):
        expiry_job_heartbeat = expiry_job_heartbeat or datetime.timedelta(minutes=1)
        expiry_lock = expiry_lock or datetime.timedelta(seconds=5)
        expiry_worker_heartbeat = expiry_worker_heartbeat or datetime.timedelta(
            minutes=1
        )
        redis_cls = redis_cls or redis.asyncio.Redis
        serializer_cls = serializer_cls or PickleSerializer

        super().__init__()

        self.expiry_job_heartbeat = expiry_job_heartbeat
        self.expiry_lock = expiry_lock
        self.expiry_worker_heartbeat = expiry_worker_heartbeat
        self.redis_cls = redis_cls
        self.redis_url = redis_url
        self.serializer_cls = serializer_cls
        self._redis = None

    async def initialize(self):
        self._redis = self.redis_cls.from_url(self.redis_url)

    def get_redis(self) -> redis.asyncio.Redis:
        if self._redis is None:
            raise RuntimeError(f"broker not initialized")
        return self._redis

    def get_transaction(self) -> redis.asyncio.client.Pipeline:
        return self.get_redis().pipeline(transaction=True)

    async def create_queued_job(self, task: Task) -> Job:
        return Job(
            id=task.id,
            signature=task.signature,
            queue=task.queue,
            return_value=None,
            status=Status.Queued,
            time_completed=None,
            time_enqueued=datetime.datetime.now(tz=datetime.timezone.utc),
            time_started=None,
        )

    def get_job_key(self, job_id: str) -> str:
        return f"{self.app_name}:jobs:{job_id}"

    def get_job_heartbeat_key(self, job_id: str) -> str:
        return f"{self.app_name}:job-heartbeats:{job_id}"

    def get_lock_key(self, lock_name: str) -> str:
        return f"{self.app_name}:locks:{lock_name}"

    def get_queue_key(self, queue: str) -> str:
        return f"{self.app_name}:queues:{queue}"

    def get_worker_heartbeat_key(self, worker_id: str) -> str:
        return f"{self.app_name}:worker-heartbeats:{worker_id}"

    @contextlib.asynccontextmanager
    async def get_lock(self, name: str) -> AsyncGenerator[None, None]:
        key = self.get_lock_key(name)
        value = str(uuid4())

        async def acquire():
            while not await self.get_redis().set(
                key, value, ex=self.expiry_lock, nx=True
            ):
                jitter = random.random()
                await asyncio.sleep(jitter)

        async def release():
            lock_value = await self.get_redis().get(key)
            if lock_value != value:
                return
            await self.get_redis().delete(key)

        await acquire()
        try:
            yield
        finally:
            await release()

    async def add_job_to_queue(self, job: Job, queue: Optional[str] = None):
        queue = queue or job.queue
        key = self.get_queue_key(queue)
        score = job.time_enqueued.timestamp()
        await self.get_redis().zadd(key, {job.id: score}, nx=True)

    async def remove_job_from_queue(self, job: Job, queue: Optional[str] = None):
        queue = queue or job.queue
        key = self.get_queue_key(queue)
        await self.get_redis().zrem(key, job.id)

    async def has_job_heartbeat(self, job: Job) -> bool:
        key = self.get_job_heartbeat_key(job.id)
        value = cast(Optional[bytes], await self.get_redis().get(key))
        return value is not None

    async def update_job_heartbeat(self, job: Job):
        key = self.get_job_heartbeat_key(job.id)
        value = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        await self.get_redis().set(key, value=value, ex=self.expiry_job_heartbeat)

    async def remove_job_heartbeat(self, job: Job):
        key = self.get_job_heartbeat_key(job.id)
        await self.get_redis().delete(key)

    async def update_worker_heartbeat(self, worker_name: str):
        key = self.get_worker_heartbeat_key(worker_name)
        value = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        await self.get_redis().set(key, value=value, ex=self.expiry_worker_heartbeat)

    async def handle_queued_job(self, job: Job):
        await asyncio.gather(self.remove_job_heartbeat(job), self.add_job_to_queue(job))

    async def handle_in_progress_job(self, job: Job):
        await asyncio.gather(self.update_job_heartbeat(job))

    async def handle_cancelled_job(self, job: Job):
        await asyncio.gather(self.remove_job_from_queue(job))

    async def handle_successful_job(self, job: Job):
        await asyncio.gather(self.remove_job_from_queue(job))

    async def handle_failed_job(self, job: Job):
        await asyncio.gather(self.remove_job_from_queue(job))

    async def get_jobs(self, *ids: str) -> list[Optional[Job]]:
        keys = list(map(self.get_job_key, ids))

        async with self.get_transaction() as transaction:
            for key in keys:
                transaction = await transaction.get(key)
            result = await transaction.execute()

        to_return = []
        for data in result:
            if data is not None:
                data = self.serializer_cls().loads(data)
            to_return.append(data)

        return to_return

    async def enqueue_task(self, task: Task) -> Job:
        (job,) = await self.get_jobs(task.id)
        if not job:
            job = await self.create_queued_job(task)

        job.return_value = None
        job.status = Status.Queued
        job.time_completed = None
        job.time_enqueued = datetime.datetime.now(tz=datetime.timezone.utc)
        job.time_started = None

        await self.update_job(job)

        return job

    async def request_job(self, worker_name: str, queues: Set[str]):
        await self.update_worker_heartbeat(worker_name)

        async with self.get_lock("request-job"):
            queue_keys = list(map(self.get_queue_key, queues))
            job_ids_bytes: list[bytes] = await self.get_redis().zunion(queue_keys)
            job_ids = map(lambda b: b.decode("utf-8"), job_ids_bytes)

            job = None
            for job_id in job_ids:
                (job,) = await self.get_jobs(job_id)
                if not job:
                    continue
                if await self.has_job_heartbeat(job):
                    continue
                break

            if not job:
                return None

            await self.update_job_heartbeat(job)
            return job

    async def update_job(self, job: Job):
        data = self.serializer_cls().dumps(job)
        key = self.get_job_key(job.id)
        await self.get_redis().set(key, data)

        if job.status == Status.Queued:
            await self.handle_queued_job(job)
        if job.status == Status.InProgress:
            await self.handle_in_progress_job(job)
        if job.status == Status.Successful:
            await self.handle_successful_job(job)
        if job.status == Status.Cancelled:
            await self.handle_cancelled_job(job)
        if job.status == Status.Failed:
            await self.handle_failed_job(job)

        event = JobStatusChangeEvent(job_id=job.id, status=job.status.value)
        await self.events.job_status_change.publish(event)
