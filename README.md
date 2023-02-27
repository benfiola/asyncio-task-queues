# asyncio-task-queues

_asyncio-task-queues_ is an asyncio-compatible, simple distributed task queue system written in python

NOTE: Still in very early stages.

---

**Documentation**: [https://benfiola.github.io/asyncio-task-queues/](https://benfiola.github.io/asyncio-task-queues/)

**Source Code**: [https://github.com/benfiola/async-task-queues](https://github.com/benfiola/async-task-queues)

---

## Features

- Native _asyncio_ implementation
  - Co-operative multi-tasking
  - Task cancellation
- Provides integrations into popular frameworks
  - _FastAPI_ [APIRouter](./asyncio_task_queues/fastapi.py)
  - _Click_ [Group](./asyncio_task_queues/click.py)
  - _Flask_ [Blueprint](./asyncio_task_queues/flask.py)
- Extensible architecture
  - Abstract [Broker](./asyncio_task_queues/broker.py) interface for unique broker implementations
  - [Middleware](./asyncio_task_queues/middleware.py) for in-flight modifications
  - [Event System](./asyncio_task_queues/event.py) for monitoring
- Strongly-typed
  - _ParamSpec_/_TypeVar_-backed task definitions
  - _Enum_-backed Queue typehints and warnings
  - _Protocol_-based event callbacks

## Quickstart

```python
from enum import Enum
from asyncio_task_queues import App, Schedule, redis


# define queues
class Queue(str, Enum):
    Default = "default"
    Other = "other"


# create broker and app
redis_url = "redis://:password@localhost:6379/0"
broker = redis.Broker(redis_url)
app = App(__name__, broker=broker, queue_default=Queue.Default)


# define a scheduled task
@app.scheduled_task(Schedule.cron("* * * * *"))
async def async_task(val: str):
    print(f"{val}")


# run a worker
async def worker():
    await app.run_worker("worker-1", {Queue.Default, Queue.Other})


# enqueue jobs as a client
async def client():
    task = app.create_task(async_task).with_args(val=1)
    await app.enqueue(task)
```
