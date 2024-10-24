from overrides import override
from typing import Any, Callable, TypeVar
from functools import wraps

from chromadb.rate_limit import RateLimitEnforcer
from chromadb.config import System

T = TypeVar("T", bound=Callable[..., Any])


class SimpleRateLimitEnforcer(RateLimitEnforcer):
    """
    A naive implementation of a quota enforcer that allows all requests.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @override
    def rate_limit(self, func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper  # type: ignore
