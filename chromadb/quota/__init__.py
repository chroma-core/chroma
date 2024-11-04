from abc import abstractmethod
from chromadb.config import Component, System


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
