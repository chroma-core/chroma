import inspect
import typing as t
from jupyter_core.utils import run_sync as _run_sync, ensure_async  # noqa


T = t.TypeVar("T")


def run_sync(coro: t.Callable[..., t.Union[T, t.Awaitable[T]]]) -> t.Callable[..., T]:
    """Wraps coroutine in a function that blocks until it has executed.

    Parameters
    ----------
    coro : coroutine-function
        The coroutine-function to be executed.

    Returns
    -------
    result :
        Whatever the coroutine-function returns.
    """
    if not inspect.iscoroutinefunction(coro):
        return t.cast(t.Callable[..., T], coro)
    return _run_sync(coro)

