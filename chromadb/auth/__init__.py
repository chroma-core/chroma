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
    Generic,
)
from dataclasses import dataclass
from starlette.requests import Request
from starlette.datastructures import Headers

from overrides import EnforceOverrides, override
from pydantic import SecretStr

from chromadb.config import (
    Component,
    System,
)

T = TypeVar("T")
S = TypeVar("S")


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


@dataclass
class ServerAuthenticationResponse(EnforceOverrides, ABC):
    """
    Represents the response from a server authentication request.
    If success = True, the user_identity field MAY be populated
    but does not have to be.
    """
    success: bool
    user_identity: Optional[UserIdentity]


class ServerAuthProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authenticate(
        self, headers: Headers
    ) -> ServerAuthenticationResponse:
        pass


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


class ServerAuthCredentialsProvider(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def validate_credentials(self,
                             credentials: AbstractCredentials[T]) -> bool:
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


class ChromaAuthzMiddleware(Component):
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def pre_process(self, request: Request) -> None:
        ...

    @abstractmethod
    def ignore_operation(self, verb: str, path: str) -> bool:
        ...
