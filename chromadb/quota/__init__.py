from abc import abstractmethod
from typing import Dict, Any

from chromadb.config import Component, System


class QuotaEnforcer(Component):
    """
    Exposes hooks to enforce quotas.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def set_context(self, context: Dict[str, Any]) -> None:
        """
        Sets the context for a given request.
        """
        pass

    @abstractmethod
    def enforce(self, action: str, **kwargs: Dict[str, Any]) -> None:
        """
        Enforces a quota.
        """
        pass
