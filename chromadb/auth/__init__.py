"""
Contains only Auth abstractions, no implementations.
"""
import base64
from abc import ABC, abstractmethod
from enum import Enum
from typing import Optional, Any, Dict

import requests
from overrides import EnforceOverrides, overrides

from chromadb.config import Component, System, Settings  # TODO remove this circular dependency
from chromadb.errors import ChromaError
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


class ServerAuthConfigurationHolder(EnforceOverrides, ABC):
    pass


class ServerAuthConfigurationProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_auth_config(self) -> Optional[ServerAuthConfigurationHolder]:
        pass


class NoopServerAuthConfigurationProvider(ServerAuthConfigurationProvider):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    def get_auth_config(self) -> Optional[ServerAuthConfigurationHolder]:
        return None


class AuthenticationError(ChromaError):

    @overrides
    def code(self) -> int:
        return 401

    @classmethod
    @overrides
    def name(cls) -> str:
        return "AuthenticationError"


class AbstractCredentials(EnforceOverrides, ABC):

    @abstractmethod
    def get_credentials(self) -> Dict[str, str]:
        """
        Returns the data encapsulated by the credentials object.
        """
        pass


class BasicAuthCredentials(AbstractCredentials):

    def __init__(self, username, password) -> None:
        self.username = username
        self.password = password

    def get_credentials(self) -> Dict[str, str]:
        return {
            "username": self.username,
            "password": self.password
        }

    @staticmethod
    def from_header(header: str) -> "BasicAuthCredentials":
        """
        Parses a basic auth header and returns a BasicAuthCredentials object.
        """
        header = header.replace("Basic ", "")
        header = header.strip()
        base64_decoded = base64.b64decode(header).decode("utf-8")
        username, password = base64_decoded.split(":")
        return BasicAuthCredentials(username, password)


class ServerAuthCredentialsProvider(EnforceOverrides, ABC):
    @abstractmethod
    def validate_credentials(self, credentials: AbstractCredentials) -> bool:
        pass


class BasicPlaintextFileServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    def __init__(self, settings: Settings) -> None:
        _file = settings.chroma_server_auth_credentials_file
        with open(_file) as f:
            _creds = f.readline().strip().split(":")
            if len(_creds) != 2:
                raise ValueError("Invalid basic auth data")
            self._creds = ":".join(_creds)

    @overrides
    def validate_credentials(self, credentials: BasicAuthCredentials) -> bool:
        _creds = credentials.get_credentials()
        return self._creds == f"{_creds['username']}:{_creds['password']}"
