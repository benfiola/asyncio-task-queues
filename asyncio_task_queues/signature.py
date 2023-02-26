import asyncio
import functools
import importlib
import inspect

from asyncio_task_queues.types import (
    Callable,
    Dict,
    Generic,
    Optional,
    ParamSpec,
    Tuple,
    TypeVar,
    cast,
)

SignaturePS = ParamSpec("SignaturePS")
SignatureRV = TypeVar("SignatureRV")


class Signature(Generic[SignaturePS, SignatureRV]):
    args: Tuple
    function: str
    kwargs: Dict

    def __init__(
        self,
        *,
        args: Optional[Tuple] = None,
        function: str,
        kwargs: Optional[Dict] = None,
    ):
        args = args or ()
        kwargs = kwargs or {}

        self.args = args
        self.function = function
        self.kwargs = kwargs

    @classmethod
    def from_function(
        cls,
        function: Callable[SignaturePS, SignatureRV],
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ) -> "Signature[SignaturePS, SignatureRV]":
        return cls(
            args=args,
            function=f"{function.__module__}:{function.__qualname__}",
            kwargs=kwargs,
        )

    async def __call__(self) -> SignatureRV:
        function = self.ensure_coroutine(self.import_function())
        return await function(*self.args, **self.kwargs)

    def with_args(
        self, *args: SignaturePS.args, **kwargs: SignaturePS.kwargs
    ) -> "Signature[SignaturePS, SignatureRV]":
        self.args = args
        self.kwargs = kwargs
        return self

    def import_function(self) -> Callable[SignaturePS, SignatureRV]:
        module_path, attr_path = self.function.split(":")
        module = importlib.import_module(module_path)
        function = module
        for attr in attr_path.split("."):
            function = getattr(function, attr)
        return cast(Callable[SignaturePS, SignatureRV], function)

    def ensure_coroutine(self, func: Callable) -> Callable:
        to_return = func

        if not inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def run_in_executor(*args, **kwargs):
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            to_return = run_in_executor

        return to_return
