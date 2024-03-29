"""
Contains only Auth abstractions, no implementations.
"""
from __future__ import annotations

import base64
from abc import ABC, abstractmethod
from enum import Enum
from typing import (
    Any,
    List,
    Optional,
    Dict,
    TypeVar,
    Tuple,
    Generic,
    Union,
)
from dataclasses import dataclass

from overrides import EnforceOverrides, override
from pydantic import SecretStr

from chromadb.config import (
    Component,
    System,
)
from chromadb.errors import ChromaError

T = TypeVar("T")
S = TypeVar("S")


class AuthInfoType(Enum):
    """
    Allowed types of authentication information.
    """
    COOKIE = "cookie"
    HEADER = "header"
    URL = "url"
    METADATA = "metadata"


class UserIdentity(EnforceOverrides, ABC):
    """
    Represents the identity of a user.
    """
    @abstractmethod
    def get_user_id(self) -> str:
        ...

    @abstractmethod
    def get_user_tenant(self) -> Optional[str]:
        ...

    @abstractmethod
    def get_user_databases(self) -> Optional[List[str]]:
        ...

    @abstractmethod
    def get_user_attributes(self) -> Optional[Dict[str, Any]]:
        ...


class SimpleUserIdentity(UserIdentity):
    """
    The simplest possible implementation of UserIdentity. This is not a dataclass
    so we can use polymorphism to just pass around UserIdentity objects.
    """
    def __init__(
        self,
        user_id: str,
        tenant: Optional[str] = None,
        databases: Optional[List[str]] = None,
        attributes: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._user_id = user_id
        self._tenant = tenant
        self._attributes = attributes
        self._databases = databases

    @override
    def get_user_id(self) -> str:
        return self._user_id

    @override
    def get_user_tenant(self) -> Optional[str]:
        return self._tenant

    @override
    def get_user_databases(self) -> Optional[List[str]]:
        return self._databases

    @override
    def get_user_attributes(self) -> Optional[Dict[str, Any]]:
        return self._attributes


class ClientAuthResponse(EnforceOverrides, ABC):
    """
    TODOBEN what exactly does this do?
    """
    @abstractmethod
    def get_auth_info_type(self) -> AuthInfoType:
        ...

    @abstractmethod
    def get_auth_info(
        self,
    # TODOBEN typing
    ) -> Union[Tuple[str, SecretStr], List[Tuple[str, SecretStr]]]:
        ...


class ClientAuthProvider(Component):
    """
    Created in the client-side system to provide authentication information which can be
    injected into a request. Use a ClientAuthProtocolAdapter to actually inject the
    credentials.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(self) -> ClientAuthResponse:
        pass


class ClientAuthCredentialsProvider(Component, Generic[T]):
    """
    Creates credentials to be used by a ClientAuthProvider. Basically ClientAuthProvider
    is a thin wrapper around this.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_credentials(self) -> T:
        pass


class ClientAuthProtocolAdapter(Component, Generic[T]):
    """
    Injects client-side credentials into a request. Uses the system's ClientAuthProvider
    to get the credentials.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def inject_credentials(self, injection_context: T) -> None:
        pass


class ServerAuthenticationRequest(EnforceOverrides, ABC, Generic[T]):
    @abstractmethod
    def get_auth_info(self, auth_info_type: AuthInfoType, auth_info_id: str) -> T:
        pass


@dataclass
class ServerAuthenticationResponse(EnforceOverrides, ABC):
    """
    Represents the response from a server authentication request. If success = True,
    the user_identity field MAY be populated but does not have to be.
    """
    success: bool
    user_identity: Optional[UserIdentity]


class ServerAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest[T]
    ) -> ServerAuthenticationResponse:
        pass


class ChromaAuthMiddleware(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, request: ServerAuthenticationRequest[T]
    ) -> ServerAuthenticationResponse:
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
    TODOBEN
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
    def from_header(header: str) -> BasicAuthCredentials:
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
        ...

    @abstractmethod
    def get_user_identity(
        self, credentials: AbstractCredentials[T]
    ) -> Optional[UserIdentity]:
        ...


class AuthzResourceTypes(str, Enum):
    DB = "db"
    COLLECTION = "collection"
    TENANT = "tenant"


class AuthzResourceActions(str, Enum):
    CREATE_DATABASE = "create_database"
    GET_DATABASE = "get_database"
    CREATE_TENANT = "create_tenant"
    GET_TENANT = "get_tenant"
    LIST_COLLECTIONS = "list_collections"
    COUNT_COLLECTIONS = "count_collections"
    GET_COLLECTION = "get_collection"
    CREATE_COLLECTION = "create_collection"
    GET_OR_CREATE_COLLECTION = "get_or_create_collection"
    DELETE_COLLECTION = "delete_collection"
    UPDATE_COLLECTION = "update_collection"
    ADD = "add"
    DELETE = "delete"
    GET = "get"
    QUERY = "query"
    COUNT = "count"
    UPDATE = "update"
    UPSERT = "upsert"
    RESET = "reset"


@dataclass
class AuthzUser:
    id: Optional[str]
    tenant: Optional[str]
    attributes: Optional[Dict[str, Any]] = None
    claims: Optional[Dict[str, Any]] = None


@dataclass
class AuthzResource:
    id: Optional[str]
    type: Optional[str]
    attributes: Optional[Dict[str, Any]] = None


# class DynamicAuthzResource:
#     id: Optional[Union[str, Callable[..., str]]]
#     type: Optional[Union[str, Callable[..., str]]]
#     attributes: Optional[Union[Dict[str, Any], Callable[..., Dict[str, Any]]]]

#     def __init__(
#         self,
#         id: Optional[Union[str, Callable[..., str]]] = None,
#         attributes: Optional[
#             Union[Dict[str, Any], Callable[..., Dict[str, Any]]]
#         ] = lambda **kwargs: {},
#         type: Optional[Union[str, Callable[..., str]]] = DEFAULT_DATABASE,
#     ) -> None:
#         self.id = id
#         self.attributes = attributes
#         self.type = type

#     def to_authz_resource(self, **kwargs: Any) -> AuthzResource:
#         return AuthzResource(
#             id=self.id(**kwargs) if callable(self.id) else self.id,
#             type=self.type(**kwargs) if callable(self.type) else self.type,
#             attributes=self.attributes(**kwargs)
#             if callable(self.attributes)
#             else self.attributes,
#         )


# class AuthzDynamicParams:
#     @staticmethod
#     def from_function_name(**kwargs: Any) -> Callable[..., str]:
#         return partial(lambda **kwargs: kwargs["function"].__name__, **kwargs)

#     @staticmethod
#     def from_function_args(**kwargs: Any) -> Callable[..., str]:
#         return partial(
#             lambda **kwargs: kwargs["function_args"][kwargs["arg_num"]], **kwargs
#         )

#     @staticmethod
#     def from_function_kwargs(**kwargs: Any) -> Callable[..., str]:
#         return partial(
#             lambda **kwargs: kwargs["function_kwargs"][kwargs["arg_name"]], **kwargs
#         )

#     @staticmethod
#     def dict_from_function_kwargs(**kwargs: Any) -> Callable[..., Dict[str, Any]]:
#         return partial(
#             lambda **kwargs: {
#                 k: kwargs["function_kwargs"][k] for k in kwargs["arg_names"]
#             },
#             **kwargs,
#         )


@dataclass
class AuthzAction:
    id: str
    attributes: Optional[Dict[str, Any]] = None


@dataclass
class AuthorizationContext:
    user: AuthzUser
    resource: AuthzResource
    action: AuthzAction


class ServerAuthorizationProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authorize(self, context: AuthorizationContext) -> bool:
        pass


class AuthorizationRequestContext(EnforceOverrides, ABC, Generic[T]):
    @abstractmethod
    def get_request(self) -> T:
        ...


class ChromaAuthzMiddleware(Component, Generic[T, S]):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def pre_process(self, request: AuthorizationRequestContext[S]) -> None:
        ...

    @abstractmethod
    def ignore_operation(self, verb: str, path: str) -> bool:
        ...

    @abstractmethod
    def instrument_server(self, app: T) -> None:
        ...


class ServerAuthorizationConfigurationProvider(Component, Generic[T]):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def get_configuration(self) -> T:
        pass
