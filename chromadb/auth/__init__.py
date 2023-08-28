"""
Contains only Auth abstractions, no implementations.
"""
import base64
import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Optional,
    Dict,
    TypeVar,
    Tuple,
    Generic,
)

from overrides import EnforceOverrides, override
from pydantic import SecretStr

from chromadb.config import (
    Component,
    System,
)
from chromadb.errors import ChromaError

logger = logging.getLogger(__name__)

T = TypeVar("T")
S = TypeVar("S")


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


class ClientAuthProtocolAdapter(Component, Generic[T]):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def inject_credentials(self, injection_context: T) -> None:
        pass


# SERVER-SIDE Abstractions


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
    def authenticate(self, request: ServerAuthenticationRequest[T]) -> bool:
        pass


class ChromaAuthMiddleware(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest[T]
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
    @override
    def code(self) -> int:
        return 401

    @classmethod
    @override
    def name(cls) -> str:
        return "AuthenticationError"


class AbstractCredentials(EnforceOverrides, ABC, Generic[T]):
    """
    The class is used by Auth Providers to encapsulate credentials received from the server
    and pass them to a ServerAuthCredentialsProvider.
    """

    @abstractmethod
    def get_credentials(self) -> Dict[str, T]:
        """
        Returns the data encapsulated by the credentials object.
        """
        pass


class SecretStrAbstractCredentials(AbstractCredentials[SecretStr]):
    @abstractmethod
    @override
    def get_credentials(self) -> Dict[str, SecretStr]:
        """
        Returns the data encapsulated by the credentials object.
        """
        pass


class BasicAuthCredentials(SecretStrAbstractCredentials):
    def __init__(self, username: SecretStr, password: SecretStr) -> None:
        self.username = username
        self.password = password

    @override
    def get_credentials(self) -> Dict[str, SecretStr]:
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
    def validate_credentials(self, credentials: AbstractCredentials[T]) -> bool:
        pass
