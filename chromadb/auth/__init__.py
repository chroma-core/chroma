from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass

from pydantic import SecretStr

from chromadb.config import Component, System


class AuthError(Exception):
    pass


ClientAuthHeaders = Dict[str, SecretStr]


class ClientAuthProvider(Component):
    """
    ClientAuthProvider is responsible for providing authentication headers for
    client requests. Client implementations (in our case, just the FastAPI
    client) must inject these headers into their requests.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(self) -> ClientAuthHeaders:
        pass


@dataclass
class UserIdentity:
    """
    UserIdentity represents the identity of a user. In general, not all fields
    will be populated, and the fields that are populated will depend on the
    authentication provider.
    """

    user_id: str
    tenant: Optional[str] = None
    databases: Optional[List[str]] = None
    attributes: Optional[Dict[str, Any]] = None
