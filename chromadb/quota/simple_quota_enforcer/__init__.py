from overrides import override
from typing import Any, Callable, TypeVar

from chromadb.quota import QuotaEnforcer
from chromadb.config import System

T = TypeVar("T", bound=Callable[..., Any])


class SimpleQuotaEnforcer(QuotaEnforcer):
    """
    A naive implementation of a rate limit enforcer that allows all requests.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @override
    def enforce(self) -> None:
        pass
