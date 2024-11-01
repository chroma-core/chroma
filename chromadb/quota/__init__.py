from abc import abstractmethod
from typing import Callable, TypeVar, Any
from chromadb.config import Component, System

T = TypeVar("T", bound=Callable[..., Any])


class QuotaProvider(Component):
    """
    Retrieves quotas for resources within a system.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)


class QuotaEnforcer(Component):
    """
    Exposes hooks to enforce quotas.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def enforce(self) -> None:
        """
        Enforces a quota.
        """
        pass
