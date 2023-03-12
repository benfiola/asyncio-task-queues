import datetime
import logging

import redis.asyncio
import redis.asyncio.client
import redis.asyncio.lock

from asyncio_task_queues.backend import Backend as BaseBackend
from asyncio_task_queues.broker import Broker as BaseBroker
from asyncio_task_queues.job import Job, Status
from asyncio_task_queues.serializer import PickleSerializer, Serializer
from asyncio_task_queues.task import Task
from asyncio_task_queues.types import List, Optional, Set, Type, cast

logger = logging.getLogger(__name__)


class Backend(BaseBackend):
    redis_cls: Type[redis.asyncio.Redis]
    redis_url: str
    serializer_cls: Type[Serializer]
    _redis: Optional[redis.asyncio.Redis]

    def __init__(
        self,
        redis_url: str,
        *,
        app_name: Optional[str] = None,
        expiry_job_cancelled: Optional[datetime.timedelta] = None,
        expiry_job_failed: Optional[datetime.timedelta] = None,
        expiry_job_successful: Optional[datetime.timedelta] = None,
        redis_cls: Optional[Type[redis.asyncio.Redis]] = None,
        serializer_cls: Optional[Type[Serializer]] = None,
    ):
        expiry_job_cancelled = expiry_job_cancelled or datetime.timedelta(days=1)
        expiry_job_failed = expiry_job_failed or datetime.timedelta(days=365)
        expiry_job_successful = expiry_job_successful or datetime.timedelta(days=1)
        redis_cls = redis_cls or redis.asyncio.Redis
        serializer_cls = serializer_cls or PickleSerializer

        super().__init__(app_name=app_name)

        self.expiry_job_cancelled = expiry_job_cancelled
        self.expiry_job_failed = expiry_job_failed
        self.expiry_job_successful = expiry_job_successful
        self.redis_cls = redis_cls
        self.redis_url = redis_url
        self.serializer_cls = serializer_cls

    async def initialize(self):
        self._redis = self.redis_cls.from_url(self.redis_url)

    def get_redis(self) -> redis.asyncio.Redis:
        if self._redis is None:
            raise RuntimeError(f"not initialized")
        return self._redis

    def get_transaction(self) -> redis.asyncio.client.Pipeline:
        return self.get_redis().pipeline(transaction=True)

    def get_job_key(self, job_id: str) -> str:
        return f"{self.get_app_name()}:jobs:{job_id}"

    async def get_jobs(self, *ids) -> List[Optional[Job]]:
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

    async def list_jobs(self) -> List[Job]:
        return []

    def get_job_expiry(self, job: Job) -> Optional[datetime.timedelta]:
        return {
            Status.Cancelled: self.expiry_job_cancelled,
            Status.Failed: self.expiry_job_failed,
            Status.Successful: self.expiry_job_successful,
        }.get(job.status, None)

    async def _save_job(self, job: Job):
        value = self.serializer_cls().dumps(job)
        key = self.get_job_key(job.id)
        async with self.get_transaction() as transaction:
            transaction = await transaction.set(key, value)

            expiry = self.get_job_expiry(job)
            if expiry is None:
                transaction = await transaction.persist(key)
            else:
                transaction = await transaction.expire(key, expiry, nx=True)

            await transaction.execute()


class Broker(BaseBroker):
    expiry_heartbeat_job: datetime.timedelta
    expiry_heartbeat_worker: datetime.timedelta
    expiry_lock: datetime.timedelta
    redis_cls: Type[redis.asyncio.Redis]
    redis_url: str

    _redis: Optional[redis.asyncio.Redis]

    def __init__(
        self,
        redis_url: str,
        *,
        app_name: Optional[str] = None,
        backend: Optional[BaseBackend] = None,
        expiry_heartbeat_job: Optional[datetime.timedelta] = None,
        expiry_heartbeat_worker: Optional[datetime.timedelta] = None,
        expiry_lock: Optional[datetime.timedelta] = None,
        redis_cls: Optional[Type[redis.asyncio.Redis]] = None,
        serializer_cls: Optional[Type[Serializer]] = None,
    ):
        expiry_lock = expiry_lock or datetime.timedelta(seconds=5)
        expiry_heartbeat_job = expiry_heartbeat_job or datetime.timedelta(minutes=1)
        expiry_heartbeat_worker = expiry_heartbeat_worker or datetime.timedelta(
            minutes=1
        )
        redis_cls = redis_cls or redis.asyncio.Redis
        serializer_cls = serializer_cls or PickleSerializer

        super().__init__(app_name=app_name, backend=backend)

        self.expiry_lock = expiry_lock
        self.expiry_heartbeat_job = expiry_heartbeat_job
        self.expiry_heartbeat_worker = expiry_heartbeat_worker
        self.redis_cls = redis_cls
        self.redis_url = redis_url
        self.serializer_cls = serializer_cls
        self._redis = None

    async def initialize(self):
        self._redis = self.redis_cls.from_url(self.redis_url)

    def get_redis(self) -> redis.asyncio.Redis:
        if self._redis is None:
            raise RuntimeError(f"not initialized")
        return self._redis

    def get_transaction(self) -> redis.asyncio.client.Pipeline:
        return self.get_redis().pipeline(transaction=True)

    def get_job_heartbeat_key(self, job_id: str) -> str:
        return f"{self.get_app_name()}:job-heartbeats:{job_id}"

    def get_lock_key(self, lock_name: str) -> str:
        return f"{self.get_app_name()}:locks:{lock_name}"

    def get_queue_key(self, queue: str) -> str:
        return f"{self.get_app_name()}:queues:{queue}"

    def get_worker_heartbeat_key(self, worker_id: str) -> str:
        return f"{self.get_app_name()}:worker-heartbeats:{worker_id}"

    def get_lock(self, name: str) -> redis.asyncio.lock.Lock:
        key = self.get_lock_key(name)
        timeout = self.expiry_lock.total_seconds()
        return redis.asyncio.lock.Lock(self.get_redis(), key, timeout=timeout)

    async def add_job_to_queue(self, job_id: str, queue: str, score: float):
        key = self.get_queue_key(queue)
        await self.get_redis().zadd(key, {job_id: score}, nx=True)

    async def remove_job_from_queue(self, job_id: str, queue: str):
        key = self.get_queue_key(queue)
        await self.get_redis().zrem(key, job_id)

    async def has_job_heartbeat(self, job_id: str) -> bool:
        key = self.get_job_heartbeat_key(job_id)
        value = cast(Optional[bytes], await self.get_redis().get(key))
        return value is not None

    async def update_job_heartbeat(self, job_id: str):
        key = self.get_job_heartbeat_key(job_id)
        value = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        await self.get_redis().set(key, value=value, ex=self.expiry_heartbeat_job)

    async def remove_job_heartbeat(self, job_id: str):
        key = self.get_job_heartbeat_key(job_id)
        await self.get_redis().delete(key)

    async def update_worker_heartbeat(self, worker_name: str):
        key = self.get_worker_heartbeat_key(worker_name)
        value = datetime.datetime.now(tz=datetime.timezone.utc).timestamp()
        await self.get_redis().set(key, value=value, ex=self.expiry_heartbeat_worker)

    async def enqueue_task(self, task: Task) -> Job:
        time_enqueued = datetime.datetime.now(tz=datetime.timezone.utc)

        (job,) = await self.get_backend().get_jobs(task.id)
        if not job:
            job = Job(
                id=task.id,
                signature=task.signature,
                queue=task.queue,
                return_value=None,
                status=Status.Queued,
                time_completed=None,
                time_enqueued=time_enqueued,
                time_started=None,
            )

        job.return_value = None
        job.status = Status.Queued
        job.time_completed = None
        job.time_enqueued = time_enqueued
        job.time_started = None

        await self.get_backend().save_job(job)
        await self.add_job_to_queue(job.id, job.queue, job.time_enqueued.timestamp())

        return job

    async def cancel_job(self, job: Job):
        (_job,) = await self.get_backend().get_jobs(job.id)
        if not _job or _job.status.is_complete():
            return

        if _job.status == Status.Queued:
            await self.remove_job_from_queue(_job.id, _job.queue)
            _job.status = Status.Cancelled
        if _job.status == Status.InProgress:
            _job.status = Status.Cancelling

        await self.get_backend().save_job(job)

    async def worker_request_job(self, worker_name: str, queues: Set[str]):
        await self.update_worker_heartbeat(worker_name)

        async with self.get_lock("request-job"):
            queue_keys = List(map(self.get_queue_key, queues))
            job_ids_bytes: List[bytes] = await self.get_redis().zunion(queue_keys)
            job_ids = map(lambda b: b.decode("utf-8"), job_ids_bytes)

            job = None
            for job_id in job_ids:
                (job,) = await self.get_backend().get_jobs(job_id)
                if not job:
                    # NOTE: edge case - job record expired, but still in queue
                    for queue_key in queue_keys:
                        await self.get_redis().zrem(queue_key, job_id)
                    continue
                if await self.has_job_heartbeat(job.id):
                    continue
                break

            if not job:
                return None

            await self.update_job_heartbeat(job.id)
            return job

    async def worker_update(self, worker_name: str):
        await self.update_worker_heartbeat(worker_name)

    async def worker_update_job(self, worker_name: str, job: Job):
        await self.update_worker_heartbeat(worker_name)
        await self.update_job_heartbeat(job.id)

        await self.get_backend().save_job(job)

        if not job.status.is_complete():
            return

        await self.remove_job_from_queue(job.id, job.queue)
        await self.remove_job_heartbeat(job.id)

        if job.status == Status.Deferred:
            await self.enqueue_task(
                Task(id=job.id, queue=job.queue, signature=job.signature)
            )
