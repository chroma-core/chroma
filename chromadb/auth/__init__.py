"""
Contains only Auth abstractions, no implementations.
"""
import base64
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from enum import Enum
from typing import Optional, Any, Dict, List, Union, Type, Callable, TypeVar

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


class AuthInfoType(Enum):
    COOKIE = "cookie"
    HEADER = "header"
    URL = "url"
    METADATA = "metadata"  # gRPC


class ServerAuthenticationRequest(EnforceOverrides, ABC):
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

    @classmethod
    @abstractmethod
    def get_type(cls) -> str:
        ...


class ServerAuthConfigurationProviderFactory:
    providers = defaultdict(dict)  # Organize by _type then by precedence
    default_provider = None
    _counter = 0

    @classmethod
    def register_provider(
        cls,
        env_vars: List[str],
        provider_class: ServerAuthConfigurationProvider,
        precedence: Optional[int] = None,
    ):
        if precedence is None:
            cls._counter += 1
            precedence = cls._counter

        cls.providers[provider_class.get_type()][tuple(env_vars)] = (
            provider_class,
            precedence,
        )

    @classmethod
    def set_default_provider(cls, provider_class):
        cls.default_provider = provider_class

    @classmethod
    def get_provider(
        cls, system: System, provider_class: Optional[Union[str, Type]] = None
    ) -> Optional[ServerAuthConfigurationProvider]:
        if provider_class:
            _provider_class = (
                get_class(provider_class, ServerAuthConfigurationProvider)
                if isinstance(provider_class, str)
                else provider_class
            )
            _provider_by_cls = [
                provider[0]
                for type_key, type_providers in cls.providers.items()
                for _, provider in type_providers.items()
                if provider[0] == _provider_class
            ]
            if len(_provider_by_cls) == 0:
                raise ValueError(f"Unknown provider class: {provider_class}")

            return system.require(_provider_by_cls[0])
        available_providers = [
            (type_key, env_vars, provider)
            for type_key, type_providers in cls.providers.items()
            for env_vars, provider in type_providers.items()
            if all(os.environ.get(env_var) for env_var in env_vars)
            or all(
                getattr(system.settings, env_var)
                for env_var in env_vars
                if hasattr(system.settings, env_var)
            )
        ]

        if not available_providers:
            if cls.default_provider:
                return cls.default_provider()
            else:
                raise ValueError("No suitable provider found!")

        # Sort first by type, then by precedence within each type
        sorted_providers = sorted(available_providers, key=lambda x: (x[0], x[2][1]))

        _, _, (provider_class, _) = sorted_providers[0]
        return system.require(provider_class)


def register_configuration_provider(*env_vars, precedence=None) -> Any:
    def decorator(cls) -> Any:
        ServerAuthConfigurationProviderFactory.register_provider(
            env_vars, cls, precedence
        )
        return cls

    return decorator


class NoopServerAuthConfigurationProvider(ServerAuthConfigurationProvider):
    """
    A no-op auth configuration provider that returns None. This is useful for cases where the auth configuration is
    not required.
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)
        ServerAuthConfigurationProviderFactory.set_default_provider(
            NoopServerAuthConfigurationProvider
        )

    @override
    def get_configuration(self) -> Optional[str]:
        return None

    @classmethod
    @override
    def get_type(cls) -> str:
        return "env"


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
        return BasicAuthCredentials(username, password)


class ServerAuthCredentialsProvider(EnforceOverrides, ABC):
    @abstractmethod
    def validate_credentials(self, credentials: AbstractCredentials) -> bool:
        pass


class ServerAuthCredentialsProviderFactory:
    providers = defaultdict(dict)  # Organize by _type then by precedence
    default_provider = None
    _counter = 0

    @classmethod
    def register_provider(
        cls,
        env_vars: List[str],
        provider_class: ServerAuthCredentialsProvider,
        precedence: Optional[int] = None,
    ) -> None:
        if precedence is None:
            cls._counter += 1
            precedence = cls._counter

        cls.providers[provider_class.get_type()][tuple(env_vars)] = (
            provider_class,
            precedence,
        )

    @classmethod
    def set_default_provider(cls, provider_class: ServerAuthCredentialsProvider):
        cls.default_provider = provider_class

    @classmethod
    def get_provider(
        cls, system: System, provider_class: Optional[Union[str, Type]] = None
    ) -> Optional[ServerAuthCredentialsProvider]:
        if provider_class:
            _provider_by_cls = [
                provider[0]
                for type_key, type_providers in cls.providers.items()
                for _, provider in type_providers.items()
                if provider[0] == provider_class
            ]
            if len(_provider_by_cls) == 0:
                raise ValueError(f"Unknown provider class: {provider_class}")

            return system.require(_provider_by_cls[0])
        available_providers = [
            (type_key, env_vars, provider)
            for type_key, type_providers in cls.providers.items()
            for env_vars, provider in type_providers.items()
            if all(os.environ.get(env_var) for env_var in env_vars)
            or all(
                getattr(system.settings, env_var)
                for env_var in env_vars
                if hasattr(system.settings, env_var)
            )
        ]

        if not available_providers:
            if cls.default_provider:
                return cls.default_provider()
            else:
                raise ValueError("No suitable provider found!")

        # Sort first by type, then by precedence within each type
        sorted_providers = sorted(available_providers, key=lambda x: (x[0], x[2][1]))

        _, _, (provider_class, _) = sorted_providers[0]
        return system.require(provider_class)


def register_credentials_provider(*env_vars, precedence=None) -> Callable:
    def decorator(cls) -> ServerAuthCredentialsProvider:
        ServerAuthConfigurationProviderFactory.register_provider(
            env_vars, cls, precedence
        )
        return cls

    return decorator


class BasicPlaintextFileServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    def __init__(self, settings: Settings) -> None:
        _file = settings.chroma_server_auth_credentials_file
        with open(_file) as f:
            _creds = f.readline().strip().split(":")
            if len(_creds) != 2:
                raise ValueError("Invalid basic auth data")
            self._creds = ":".join(_creds)

    @override
    def validate_credentials(self, credentials: AbstractCredentials) -> bool:
        _creds = credentials.get_credentials()
        return self._creds == f"{_creds['username']}:{_creds['password']}"
