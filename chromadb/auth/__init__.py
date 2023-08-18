"""
Contains only Auth abstractions, no implementations.
"""
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional

import requests
from overrides import EnforceOverrides

from chromadb.config import Component, System  # TODO remove this circular dependency
from chromadb.utils import get_class


# Re-export types from chromadb
# __all__ = ["BasicAuthClientProvider", "BasicAuthServerProvider"]


class ClientAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(self, session: requests.Session) -> None:
        pass


_provider_registry = {
    # TODO we need a better way to store, update and validate this registry with new providers being added
    [
        "basic",
        "chromadb.auth.basic.BasicAuthClientProvider",
    ]: "chromadb.auth.basic.BasicAuthClientProvider",
}


def resolve_client_auth_provider(classOrName) -> "ClientAuthProvider":
    _cls = [
        cls
        for short_hand_list, cls in _provider_registry.items()
        if classOrName in short_hand_list
    ]
    if len(_cls) == 0:
        raise ValueError(f"Unknown client auth provider: {classOrName}")
    return get_class(_cls[0], ClientAuthProvider)


### SERVER-SIDE Abstractions


class AuthInfoType(Enum):
    COOKIE = "cookie"
    HEADER = "header"
    URL = "url"
    METADATA = "metadata"  # gRPC


class ServerAuthenticationRequest(EnforceOverrides, ABC):
    @abstractmethod
    def get_auth_info(
        self, auth_info_type: AuthInfoType, auth_info_id: Optional[str] = None
    ) -> str:
        pass


class ServerAuthenticationResponse(EnforceOverrides, ABC):
    def success(self) -> bool:
        raise NotImplementedError()


class ServerAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest
    ) -> ServerAuthenticationResponse:
        pass


class ChromaAuthMiddleware(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest
    ) -> Optional[ServerAuthenticationResponse]:
        pass
