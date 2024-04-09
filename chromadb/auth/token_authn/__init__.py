import logging
import string
from enum import Enum
from typing import List, Optional, TypedDict, cast, TypeVar

from fastapi import HTTPException
from overrides import override
from pydantic import SecretStr
import yaml

from chromadb.auth import (
    ServerAuthenticationProvider,
    ClientAuthProvider,
    ClientAuthHeaders,
    UserIdentity,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from starlette.datastructures import Headers
from typing import Dict

T = TypeVar("T")

logger = logging.getLogger(__name__)

__all__ = ["TokenAuthenticationServerProvider", "TokenAuthClientProvider"]


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


class TokenAuthenticationServerProvider(ServerAuthenticationProvider):
    """
    Server authentication provider for token-based auth. The provider will
    - On initialization, read the users from the file specified in
        `chroma_server_authn_credentials_file`. This file must be a well-formed
        YAML file with a top-level array called `users`. Each user must have
        an `id` field and a `tokens` (string array) field.
    - On each request, check the token in the header specified by
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

        creds = self.read_creds_or_creds_file()
        self._users = cast(List[User], yaml.safe_load(creds)["users"])

        self._token_user_mapping: Dict[str, User] = {}
        for user in self._users:
            if "tokens" not in user:
                raise ValueError("User missing tokens")
            if "tenant" not in user:
                user["tenant"] = "*"
            if "databases" not in user:
                user["databases"] = ["*"]
            for token in user["tokens"]:
                _check_token(token)
                if token in self._token_user_mapping and \
                        self._token_user_mapping[token] != user:
                    raise ValueError(
                        f"Token {token} already in use: wanted to use it for "
                        f"user {user['id']} but it's already in use by "
                        f"user {self._token_user_mapping[token]}"
                    )
                self._token_user_mapping[token] = user

    @trace_method("TokenAuthenticationServerProvider.authenticate",
                  OpenTelemetryGranularity.ALL)
    @override
    def authenticate_or_raise(
        self, headers: Headers
    ) -> UserIdentity:
        try:
            token = headers[
                self._token_transport_header.value
            ]
            if (self._token_transport_header ==
                    TokenTransportHeader.AUTHORIZATION):
                if not token.startswith("Bearer "):
                    raise HTTPException(status_code=401, detail="Unauthorized")
                token = token.replace("Bearer ", "")

            token = token.strip()
            _check_token(token)

            if token not in self._token_user_mapping:
                raise HTTPException(status_code=401, detail="Unauthorized")

            user_identity = UserIdentity(
                user_id=self._token_user_mapping[token]["id"],
                tenant=self._token_user_mapping[token]["tenant"],
                databases=self._token_user_mapping[token]["databases"],
            )
            return user_identity
        except Exception as e:
            logger.error(
                "TokenAuthenticationServerProvider.authenticate "
                f"failed: {repr(e)}"
            )
            raise HTTPException(status_code=401, detail="Unauthorized")
