import string
from enum import Enum

from overrides import override
from pydantic import SecretStr

from chromadb.auth import ClientAuthHeaders, ClientAuthProvider
from chromadb.config import System

__all__ = [
    "TokenAuthClientProvider",
    "TokenTransportHeader",
]


class TokenTransportHeader(str, Enum):
    """
    Accceptable token transport headers.
    """

    # I don't love having this enum here -- it's weird to have an enum
    # for just two values and it's weird to have users pass X_CHROMA_TOKEN
    # to configure "x-chroma-token". But I also like having a single source
    # of truth, so 🤷🏻‍♂️
    AUTHORIZATION = "Authorization"
    X_CHROMA_TOKEN = "X-Chroma-Token"


valid_token_chars = set(string.digits + string.ascii_letters + string.punctuation)


def _check_token(token: str) -> None:
    token_str = str(token)
    if not all(c in valid_token_chars for c in token_str):
        raise ValueError(
            "Invalid token. Must contain only ASCII letters, digits, and punctuation."
        )


allowed_token_headers = [
    TokenTransportHeader.AUTHORIZATION.value,
    TokenTransportHeader.X_CHROMA_TOKEN.value,
]


def _check_allowed_token_headers(token_header: str) -> None:
    if token_header not in allowed_token_headers:
        raise ValueError(
            f"Invalid token transport header: {token_header}. "
            f"Must be one of {allowed_token_headers}"
        )


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
        self._token = SecretStr(str(system.settings.chroma_client_auth_credentials))
        _check_token(self._token.get_secret_value())

        if system.settings.chroma_auth_token_transport_header:
            _check_allowed_token_headers(
                system.settings.chroma_auth_token_transport_header
            )
            self._token_transport_header = TokenTransportHeader(
                system.settings.chroma_auth_token_transport_header
            )
        else:
            self._token_transport_header = TokenTransportHeader.AUTHORIZATION

    @override
    def authenticate(self) -> ClientAuthHeaders:
        val = self._token.get_secret_value()
        if self._token_transport_header == TokenTransportHeader.AUTHORIZATION:
            val = f"Bearer {val}"
        return {
            self._token_transport_header.value: SecretStr(val),
        }
