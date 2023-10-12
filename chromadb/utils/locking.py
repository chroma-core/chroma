import threading
from functools import wraps
from typing import Callable

lock = threading.RLock()


def synchronized(function: Callable) -> Callable:  # type: ignore
    """
    Decorator to synchronize a function call on a global lock. This allows us to 
    ensure thread safety while allowing multiple persistent or ephemeral clients to 
    be created.
    """
    @wraps(function)
    def wrapped(*args, **kwargs):
        with lock:
            return function(*args, **kwargs)
    return wrapped
