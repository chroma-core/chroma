import inspect
import asyncio
from typing import Any, Callable, Coroutine, TypeVar
from typing_extensions import ParamSpec


P = ParamSpec("P")
R = TypeVar("R")


def async_to_sync(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, R]:
    """A function decorator that converts an async function to a sync function.

    This should generally not be used in production code paths.
    """

    def sync_wrapper(*args, **kwargs):  # type: ignore
        loop = None

        def new_event_loop():
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            return new_loop

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None

        if loop is None or loop.is_closed():
            loop = new_event_loop()

        try:
            if loop.is_running():
                return func(*args, **kwargs)
        except RuntimeError:
            loop = new_event_loop()

        result = loop.run_until_complete(func(*args, **kwargs))

        def convert_result(result: Any) -> Any:
            if isinstance(result, list):
                return [convert_result(r) for r in result]

            if isinstance(result, object):
                return async_class_to_sync(result)

            if callable(result):
                return async_to_sync(result)

            return result

        return convert_result(result)

    return sync_wrapper


T = TypeVar("T")


def async_class_to_sync(cls: T) -> T:
    """A decorator that converts a class with async methods to a class with sync methods.

    This should generally not be used in production code paths.
    """
    for attr, value in inspect.getmembers(cls):
        if (
            callable(value)
            and inspect.iscoroutinefunction(value)
            and not attr.startswith("__")
        ):
            setattr(cls, attr, async_to_sync(value))

    return cls
