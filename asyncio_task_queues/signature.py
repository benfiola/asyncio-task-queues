import asyncio
import functools
import importlib
import inspect

from asyncio_task_queues.context import Context, InjectedContext
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

PS = ParamSpec("PS")
RV = TypeVar("RV")


class Signature(Generic[PS, RV]):
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
        function: Callable[PS, RV],
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
    ) -> "Signature[PS, RV]":
        return cls(
            args=args,
            function=f"{function.__module__}:{function.__qualname__}",
            kwargs=kwargs,
        )

    async def __call__(self, context: Optional[Context] = None) -> RV:
        function = self.ensure_coroutine(self.import_function())

        signature = inspect.signature(function)
        bound = signature.bind_partial(*self.args, **self.kwargs)
        for parameter in signature.parameters.values():
            if parameter.annotation == Context:
                context = context or bound.arguments[parameter.name]
                if not context:
                    raise ValueError("context not provided")
                bound.arguments[parameter.name] = context

        return await function(**bound.arguments)

    def with_args(self, *args: PS.args, **kwargs: PS.kwargs) -> "Signature[PS, RV]":
        self.args = args
        self.kwargs = kwargs
        return self

    def import_function(self) -> Callable[PS, RV]:
        module_path, attr_path = self.function.split(":")
        module = importlib.import_module(module_path)
        function = module
        for attr in attr_path.split("."):
            function = getattr(function, attr)
        return cast(Callable[PS, RV], function)

    def ensure_coroutine(self, func: Callable) -> Callable:
        to_return = func

        if not inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def run_in_executor(*args, **kwargs):
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            to_return = run_in_executor

        return to_return
