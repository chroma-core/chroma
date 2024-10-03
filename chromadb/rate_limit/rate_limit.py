from abc import abstractmethod
from typing import Callable, TypeVar, Any
from chromadb.config import Component

T = TypeVar("T", bound=Callable[..., Any])


class RateLimitEnforcer(Component):
    @abstractmethod
    def rate_limit(self, func: T) -> T:
        pass
