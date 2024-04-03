from __future__ import annotations

from abc import abstractmethod
from enum import Enum
from typing import (
    Any,
    List,
    Optional,
    Dict,
    TypeVar,
)
from dataclasses import dataclass
from starlette.datastructures import Headers

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
    # This can be used for any additional auth context which needs to be
    # propagated from the authentication provider to the authorization
    # provider.
    attributes: Optional[Dict[str, Any]] = None


class ServerAuthenticationProvider(Component):
    """
    ServerAuthenticationProvider is responsible for authenticating requests. If
    a ServerAuthenticationProvider is configured, it will be called by the
    server to authenticate requests. If no ServerAuthenticationProvider is
    configured, all requests will be authenticated.

    The ServerAuthenticationProvider should return a UserIdentity object if the
    request is authenticated for use by the ServerAuthorizationProvider.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._ignore_auth_paths: Dict[
            str, List[str]
        ] = system.settings.chroma_server_auth_ignore_paths

    @abstractmethod
    def authenticate(
        self, headers: Headers
    ) -> Optional[UserIdentity]:
        pass

    def ignore_operation(self, verb: str, path: str) -> bool:
        if (
            path in self._ignore_auth_paths.keys()
            and verb.upper() in self._ignore_auth_paths[path]
        ):
            return True
        return False


class AuthzAction(str, Enum):
    """
    The set of actions that can be authorized by the authorization provider.
    """
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


class ServerAuthorizationProvider(Component):
    """
    ServerAuthorizationProvider is responsible for authorizing requests. If a
    ServerAuthorizationProvider is configured, it will be called by the server
    to authorize requests. If no ServerAuthorizationProvider is configured, all
    requests will be authorized.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)

    @abstractmethod
    def authorize(self,
                  user: UserIdentity,
                  action: AuthzAction,
                  resource: str) -> bool:
        pass
