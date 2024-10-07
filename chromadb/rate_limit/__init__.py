from abc import abstractmethod
from typing import Callable, TypeVar, Any
from chromadb.config import Component, System

T = TypeVar("T", bound=Callable[..., Any])


class RateLimitEnforcer(Component):
    """
    Rate limit enforcer. Implemented as a wrapper around server functions to
    block requests if rate limits are exceeded.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def rate_limit(self, func: T) -> T:
        pass
