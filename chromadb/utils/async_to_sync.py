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
                    if asyncio.get_event_loop().is_running():
                        return func(*args, **kwargs)
                    else:
                        loop = asyncio.get_event_loop()
                        result = loop.run_until_complete(func(*args, **kwargs))

                    return result

                return sync_wrapper

            setattr(cls, attr, construct_wrapper(value))

    # todo: patch __enter__ and __exit__ methods?

    # if hasattr(cls, "__aenter__"):

    #     def __aenter__(self):
    #         print("running aenter")
    #         loop = asyncio.get_event_loop()
    #         return loop.run_until_complete(self.__aenter__())

    #     setattr(cls, "__enter__", __aenter__)

    # if hasattr(cls, "__aexit__"):
    #     print("patching aexit")

    #     def __aexit__(self, exc_type, exc_value, traceback):
    #         print("running exit")
    #         loop = asyncio.get_event_loop()
    #         return loop.run_until_complete(
    #             self.__aexit__(exc_type, exc_value, traceback)
    #         )

    #     setattr(cls, "__exit__", __aexit__)

    return cls
