"""
Contains only Auth abstractions, no implementations.
"""
import base64
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import (
    Optional,
    Any,
    Dict,
    List,
    Union,
    Type,
    Callable,
    TypeVar,
    Tuple,
    Generic,
    Mapping,
)

import bcrypt
import requests
from overrides import EnforceOverrides, overrides, override
from pydantic import SecretStr

from chromadb.config import (
    Component,
    System,
    Settings,
    get_fqn,
)  # TODO remove this circular dependency
from chromadb.errors import ChromaError
from chromadb.utils import get_class

T = TypeVar("T")
S = TypeVar("S")


# Re-export types from chromadb
# __all__ = ["BasicAuthClientProvider", "BasicAuthServerProvider"]


class AuthInfoType(Enum):
    COOKIE = "cookie"
    HEADER = "header"
    URL = "url"
    METADATA = "metadata"  # gRPC


class ClientAuthResponse(EnforceOverrides, ABC):
    @abstractmethod
    def get_auth_info_type(self) -> AuthInfoType:
        ...

    @abstractmethod
    def get_auth_info(self) -> Tuple[str, SecretStr]:
        ...


class ClientAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(self) -> ClientAuthResponse:
        pass


class ClientAuthConfigurationProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_configuration(self) -> Optional[T]:
        pass


class ClientAuthCredentialsProvider(Component, Generic[T]):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_credentials(self) -> T:
        pass


class ClientAuthProtocolAdapter(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def inject_credentials(self, injection_context: T) -> None:
        pass


class RequestsClientAuthProtocolAdapter(ClientAuthProtocolAdapter):
    class _Session(requests.Session):
        _protocol_adapter: ClientAuthProtocolAdapter

        def __init__(self, protocol_adapter: ClientAuthProtocolAdapter) -> None:
            super().__init__()
            self._protocol_adapter = protocol_adapter

        @override
        def send(
            self, request: requests.PreparedRequest, **kwargs: Any
        ) -> requests.Response:
            self._protocol_adapter.inject_credentials(request)
            return super().send(request, **kwargs)

    _session: _Session
    _auth_provider: ClientAuthProvider

    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_client_auth_provider")
        self._auth_provider = system.require(
            get_class(system.settings.chroma_client_auth_provider, ClientAuthProvider)
        )
        self._session = self._Session(self)
        self._auth_header = self._auth_provider.authenticate()

    @property
    def session(self) -> requests.Session:
        return self._session

    @override
    def inject_credentials(self, injection_context: requests.PreparedRequest) -> None:
        if self._auth_header.get_auth_info_type() == AuthInfoType.HEADER:
            _header_info = self._auth_header.get_auth_info()
            injection_context.headers[_header_info[0]] = _header_info[1]
        else:
            raise ValueError(
                f"Unsupported auth type: {self._auth_header.get_auth_info_type()}"
            )


class ConfigurationClientAuthCredentialsProvider(ClientAuthCredentialsProvider):
    _creds: SecretStr

    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_client_auth_credentials")
        self._creds = SecretStr(system.settings.chroma_client_auth_credentials)

    @override
    def get_credentials(self) -> SecretStr:
        return self._creds


_provider_registry = {
    # TODO we need a better way to store, update and validate this registry with new providers being added
    (
        "basic",
        "chromadb.auth.basic.BasicAuthClientProvider",
    ): "chromadb.auth.basic.BasicAuthClientProvider",
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


class ServerAuthenticationRequest(EnforceOverrides, ABC, Generic[T]):
    @abstractmethod
    def get_auth_info(
        self, auth_info_type: AuthInfoType, auth_info_id: Optional[str] = None
    ) -> T:
        """
        This method should return the necessary auth info based on the type of authentication (e.g. header, cookie, url)
         and a given id for the respective auth type (e.g. name of the header, cookie, url param).

        :param auth_info_type: The type of auth info to return
        :param auth_info_id: The id of the auth info to return
        :return: The auth info which can be specific to the implementation
        """
        pass


class ServerAuthenticationResponse(EnforceOverrides, ABC):
    def success(self) -> bool:
        raise NotImplementedError()


class ServerAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(self, request: ServerAuthenticationRequest) -> bool:
        pass


class ChromaAuthMiddleware(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest
    ) -> Optional[ServerAuthenticationResponse]:
        ...

    @abstractmethod
    def ignore_operation(self, verb: str, path: str) -> bool:
        ...

    @abstractmethod
    def instrument_server(self, app: T) -> None:
        ...


class ServerAuthConfigurationProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_configuration(self) -> Optional[T]:
        pass


class AuthenticationError(ChromaError):
    @overrides
    def code(self) -> int:
        return 401

    @classmethod
    @overrides
    def name(cls) -> str:
        return "AuthenticationError"


class AbstractCredentials(EnforceOverrides, ABC):
    """
    The class is used by Auth Providers to encapsulate credentials received from the server
    and pass them to a ServerAuthCredentialsProvider.
    """

    @abstractmethod
    def get_credentials(self) -> Dict[str, Union[str, int, float, bool, SecretStr]]:
        """
        Returns the data encapsulated by the credentials object.
        """
        pass


class BasicAuthCredentials(AbstractCredentials):
    def __init__(self, username, password) -> None:
        self.username = username
        self.password = password

    @override
    def get_credentials(self) -> Dict[str, Union[str, int, float, bool, SecretStr]]:
        return {"username": self.username, "password": self.password}

    @staticmethod
    def from_header(header: str) -> "BasicAuthCredentials":
        """
        Parses a basic auth header and returns a BasicAuthCredentials object.
        """
        header = header.replace("Basic ", "")
        header = header.strip()
        base64_decoded = base64.b64decode(header).decode("utf-8")
        username, password = base64_decoded.split(":")
        return BasicAuthCredentials(SecretStr(username), SecretStr(password))


class ServerAuthCredentialsProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def validate_credentials(self, credentials: AbstractCredentials) -> bool:
        pass


class HtpasswdServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    _creds: List[SecretStr]

    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_server_auth_credentials_file")
        _file = system.settings.chroma_server_auth_credentials_file
        with open(_file) as f:
            self._creds = [SecretStr(v) for v in f.readline().strip().split(":")]
            if len(self._creds) != 2:
                raise ValueError(
                    f"Invalid Htpasswd credentials file [{_file}]. Must be <username>:<bcrypt passwd>."
                )

    @override
    def validate_credentials(self, credentials: AbstractCredentials) -> bool:
        _creds = credentials.get_credentials()
        return _creds["username"].get_secret_value() == self._creds[
            0
        ].get_secret_value() and bcrypt.checkpw(
            _creds["password"].get_secret_value().encode("utf-8"),
            self._creds[1].get_secret_value().encode("utf-8"),
        )
