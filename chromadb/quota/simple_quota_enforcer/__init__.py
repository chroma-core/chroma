from overrides import override
from typing import Any, Callable, TypeVar, Dict

from chromadb.quota import QuotaEnforcer
from chromadb.config import System

T = TypeVar("T", bound=Callable[..., Any])


class SimpleQuotaEnforcer(QuotaEnforcer):
    """
    A naive implementation of a quota enforcer that allows all requests.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @override
    def set_context(self, context: Dict[str, Any]) -> None:
        pass

    @override
    def enforce(self, action: str, **kwargs: Dict[str, Any]) -> None:
        pass
