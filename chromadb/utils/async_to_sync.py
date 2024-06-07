import inspect
import asyncio
from typing import TypeVar

T = TypeVar("T")


def async_class_to_sync(cls: T) -> T:
    """A decorator that converts a class with async methods to a class with sync methods."""
    for attr, value in inspect.getmembers(cls):
        if (
            callable(value)
            and inspect.iscoroutinefunction(value)
            and not attr.startswith("__")
        ):
            # (need an extra wrapper to capture the current value/func)
            def construct_wrapper(func):
                def sync_wrapper(*args, **kwargs):
                    loop = None
                    try:
                        loop = asyncio.get_event_loop()
                    except RuntimeError:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                    if loop.is_running():
                        return func(*args, **kwargs)

                    result = loop.run_until_complete(func(*args, **kwargs))

                    # todo: super hacky, is there a better pattern to use?
                    if isinstance(result, list):
                        return [async_class_to_sync(r) for r in result]

                    if isinstance(result, object):
                        return async_class_to_sync(result)

                    return result

                return sync_wrapper

            setattr(cls, attr, construct_wrapper(value))

    return cls
