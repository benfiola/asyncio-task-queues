import importlib

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
        cls, function: Callable[SignaturePS, SignatureRV]
    ) -> "Signature[SignaturePS, SignatureRV]":
        return cls(
            function=f"{function.__module__}:{function.__qualname__}",
        )

    def import_function(self) -> Callable[SignaturePS, SignatureRV]:
        module_path, attr_path = self.function.split(":")
        module = importlib.import_module(module_path)
        function = module
        for attr in attr_path.split("."):
            function = getattr(function, attr)
        return cast(Callable[SignaturePS, SignatureRV], function)
