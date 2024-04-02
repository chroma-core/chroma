import logging
import string
from enum import Enum
from typing import List, Optional, TypedDict, cast, TypeVar

from overrides import override
from pydantic import SecretStr
import yaml

from chromadb.auth import (
    ServerAuthProvider,
    ClientAuthProvider,
    ClientAuthHeaders,
    ServerAuthenticationResponse,
    UserIdentity,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from starlette.datastructures import Headers

T = TypeVar("T")

logger = logging.getLogger(__name__)

__all__ = ["TokenAuthServerProvider", "TokenAuthClientProvider"]


class TokenTransportHeader(Enum):
    """
    Accceptable token transport headers.
    """
    # I don't love having this enum here -- it's weird to have an enum
    # for just two values and it's weird to have users pass X_CHROMA_TOKEN
    # to configure "x-chroma-token". But I also like having a single source
    # of truth, so 🤷🏻‍♂️
    AUTHORIZATION = "Authorization"
    X_CHROMA_TOKEN = "X-Chroma-Token"


def _check_token(token: str) -> None:
    token_str = str(token)
    if not all(
        c in string.digits + string.ascii_letters + string.punctuation
        for c in token_str
    ):
        raise ValueError("Invalid token. Must contain \
                         only ASCII letters and digits.")


class TokenAuthClientProvider(ClientAuthProvider):
    """
    Client auth provider for token-based auth. Header key will be either
    "Authorization" or "X-Chroma-Token" depending on
    `chroma_auth_token_transport_header`. If the header is "Authorization",
    the token is passed as a bearer token.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings

        system.settings.require("chroma_client_auth_credentials")
        self._token = SecretStr(
            str(system.settings.chroma_client_auth_credentials)
        )
        _check_token(self._token.get_secret_value())

        if system.settings.chroma_auth_token_transport_header:
            self._token_transport_header = TokenTransportHeader[
                str(system.settings.chroma_auth_token_transport_header)
            ]
        else:
            self._token_transport_header = TokenTransportHeader.AUTHORIZATION

    @override
    def authenticate(self) -> ClientAuthHeaders:
        val = self._token.get_secret_value()
        if self._token_transport_header == TokenTransportHeader.AUTHORIZATION:
            val = f"Bearer {val}"
        return {
            self._token_transport_header.value:
            SecretStr(val),
        }


class User(TypedDict):
    """
    A simple User class for use in this module only. If you need a generic
    way to represent a User, please use UserIdentity as this class keeps
    track of sensitive tokens.
    """
    id: str
    role: str
    tenant: Optional[str]
    databases: Optional[List[str]]
    tokens: List[str]


class TokenAuthServerProvider(ServerAuthProvider):
    """
    Server authentication provider for token-based auth. The server will
    - Read the users from the file specified in
        `chroma_server_auth_credentials_file`
    - Check the token in the header specified by
        `chroma_auth_token_transport_header`. If the configured header is
        "Authorization", the token is expected to be a bearer token.
    - If the token is valid, the server will return the user identity
        associated with the token.
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        if system.settings.chroma_auth_token_transport_header:
            self._token_transport_header = TokenTransportHeader[
                str(system.settings.chroma_auth_token_transport_header)
            ]
        else:
            self._token_transport_header = TokenTransportHeader.AUTHORIZATION

        if not system.settings.chroma_server_auth_credentials_file:
            raise ValueError("chroma_server_auth_credentials_file not set")

        users_file = str(
            system.settings.chroma_server_auth_credentials_file
        )
        with open(users_file) as f:
            self._users = cast(List[User], yaml.safe_load(f)["users"])

        self._token_user_mapping = {}
        for user in self._users:
            if "tokens" not in user:
                raise ValueError("User missing tokens")
            if "tenant" not in user:
                user["tenant"] = "*"
            if "databases" not in user:
                user["databases"] = ["*"]
            for token in user["tokens"]:
                _check_token(token)
                if token in self._token_user_mapping:
                    raise ValueError(
                        f"Token ${token} already in use: wanted to use it for "
                        f"user ${user['id']} but it's already in use by "
                        f"user ${self._token_user_mapping[token]}"
                    )
                self._token_user_mapping[token] = user

    @trace_method("TokenAuthServerProvider.authenticate",
                  OpenTelemetryGranularity.ALL)
    @override
    def authenticate(
        self, headers: Headers
    ) -> ServerAuthenticationResponse:
        try:
            token = headers[
                self._token_transport_header.value
            ]
            if (self._token_transport_header ==
                    TokenTransportHeader.AUTHORIZATION):
                if not token.startswith("Bearer "):
                    return ServerAuthenticationResponse(False, None)
                token = token.replace("Bearer ", "")

            token = token.strip()
            _check_token(token)

            if token not in self._token_user_mapping:
                return ServerAuthenticationResponse(False, None)

            user_identity = UserIdentity(
                user_id=self._token_user_mapping[token]["id"],
                tenant=self._token_user_mapping[token]["tenant"],
                databases=self._token_user_mapping[token]["databases"],
            )
            return ServerAuthenticationResponse(
                True,
                user_identity
            )
        except Exception as e:
            logger.error(
                f"TokenAuthServerProvider.authenticate failed: {repr(e)}"
            )
            return ServerAuthenticationResponse(False, None)
