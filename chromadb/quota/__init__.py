from abc import abstractmethod
from typing import Callable, TypeVar, Any, Dict
from chromadb.config import Component, System

T = TypeVar("T", bound=Callable[..., Any])


class QuotaProvider(Component):
    """
    Retrieves quotas for resources within a system.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @property
    @abstractmethod
    def quota_rules(self) -> Dict[str, int]:
        """
        A dictionary mapping a quota rule to its integer value.
        """
        pass


class QuotaEnforcer(Component):
    """
    Exposes hooks to enforce quota rules. A distinction is drawn between
    general quotas and rate limits, which are a specific type of quota.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def enforce(self) -> None:
        """
        Enforces general quota rules.
        """
        pass

    @abstractmethod
    def rate_limit(self, func: T) -> T:
        """
        Enforces rate limits. Implemented as a wrapper.
        """
        pass
