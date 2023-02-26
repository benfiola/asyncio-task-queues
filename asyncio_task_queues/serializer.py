import pickle
from abc import ABC, abstractmethod

from asyncio_task_queues.types import Any


class Serializer(ABC):
    @abstractmethod
    def loads(self, value: bytes) -> Any:
        ...

    @abstractmethod
    def dumps(self, value: Any) -> bytes:
        ...


class PickleSerializer(Serializer):
    def loads(self, value: bytes) -> Any:
        return pickle.loads(value)

    def dumps(self, value: Any) -> bytes:
        return pickle.dumps(value)
