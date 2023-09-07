import logging
import string
from enum import Enum
from typing import Tuple, Any, cast, Dict, TypeVar

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    ServerAuthProvider,
    ClientAuthProvider,
    ServerAuthenticationRequest,
    ServerAuthCredentialsProvider,
    AuthInfoType,
    ClientAuthCredentialsProvider,
    ClientAuthResponse,
    SecretStrAbstractCredentials,
    AbstractCredentials,
)
from chromadb.auth.registry import register_provider, resolve_provider
from chromadb.config import System
from chromadb.utils import get_class

T = TypeVar("T")

logger = logging.getLogger(__name__)

__all__ = ["TokenAuthServerProvider", "TokenAuthClientProvider"]

_token_transport_headers = ["Authorization", "X-Chroma-Token"]


class TokenTransportHeader(Enum):
    AUTHORIZATION = "Authorization"
    X_CHROMA_TOKEN = "X-Chroma-Token"


class TokenAuthClientAuthResponse(ClientAuthResponse):
    _token_transport_header: TokenTransportHeader

    def __init__(
        self,
        credentials: SecretStr,
        token_transport_header: TokenTransportHeader = TokenTransportHeader.AUTHORIZATION,
    ) -> None:
        self._credentials = credentials
        self._token_transport_header = token_transport_header

    @override
    def get_auth_info_type(self) -> AuthInfoType:
        return AuthInfoType.HEADER

    @override
    def get_auth_info(self) -> Tuple[str, SecretStr]:
        if self._token_transport_header == TokenTransportHeader.AUTHORIZATION:
            return "Authorization", SecretStr(
                f"Bearer {self._credentials.get_secret_value()}"
            )
        elif self._token_transport_header == TokenTransportHeader.X_CHROMA_TOKEN:
            return "X-Chroma-Token", SecretStr(
                f"{self._credentials.get_secret_value()}"
            )
        else:
            raise ValueError(
                f"Invalid token transport header: {self._token_transport_header}"
            )


def check_token(token: str) -> None:
    token_str = str(token)
    if not all(
        c in string.digits + string.ascii_letters + string.punctuation
        for c in token_str
    ):
        raise ValueError("Invalid token. Must contain only ASCII letters and digits.")


@register_provider("token_config")
class TokenConfigServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    _token: SecretStr

    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_server_auth_credentials")
        token_str = str(system.settings.chroma_server_auth_credentials)
        check_token(token_str)
        self._token = SecretStr(token_str)

    @override
    def validate_credentials(self, credentials: AbstractCredentials[T]) -> bool:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        if "token" not in _creds:
            logger.error("Returned credentials do not contain token")
            return False
        return _creds["token"].get_secret_value() == self._token.get_secret_value()


class TokenAuthCredentials(SecretStrAbstractCredentials):
    _token: SecretStr

    def __init__(self, token: SecretStr) -> None:
        self._token = token

    @override
    def get_credentials(self) -> Dict[str, SecretStr]:
        return {"token": self._token}

    @staticmethod
    def from_header(
        header: str,
        token_transport_header: TokenTransportHeader = TokenTransportHeader.AUTHORIZATION,
    ) -> "TokenAuthCredentials":
        """
        Extracts token from header and returns a TokenAuthCredentials object.
        """
        if token_transport_header == TokenTransportHeader.AUTHORIZATION:
            header = header.replace("Bearer ", "")
            header = header.strip()
            token = header
        elif token_transport_header == TokenTransportHeader.X_CHROMA_TOKEN:
            header = header.strip()
            token = header
        else:
            raise ValueError(
                f"Invalid token transport header: {token_transport_header}"
            )
        return TokenAuthCredentials(SecretStr(token))


@register_provider("token")
class TokenAuthServerProvider(ServerAuthProvider):
    _credentials_provider: ServerAuthCredentialsProvider
    _token_transport_header: TokenTransportHeader = TokenTransportHeader.AUTHORIZATION

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        system.settings.require("chroma_server_auth_credentials_provider")
        self._credentials_provider = cast(
            ServerAuthCredentialsProvider,
            system.require(
                resolve_provider(
                    str(system.settings.chroma_server_auth_credentials_provider),
                    ServerAuthCredentialsProvider,
                )
            ),
        )
        if system.settings.chroma_server_auth_token_transport_header:
            self._token_transport_header = TokenTransportHeader[
                str(system.settings.chroma_server_auth_token_transport_header)
            ]

    @override
    def authenticate(self, request: ServerAuthenticationRequest[Any]) -> bool:
        try:
            _auth_header = request.get_auth_info(
                AuthInfoType.HEADER, self._token_transport_header.value
            )
            return self._credentials_provider.validate_credentials(
                TokenAuthCredentials.from_header(
                    _auth_header, self._token_transport_header
                )
            )
        except Exception as e:
            logger.error(f"TokenAuthServerProvider.authenticate failed: {repr(e)}")
            return False


@register_provider("token")
class TokenAuthClientProvider(ClientAuthProvider):
    _credentials_provider: ClientAuthCredentialsProvider[Any]
    _token_transport_header: TokenTransportHeader = TokenTransportHeader.AUTHORIZATION

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings

        system.settings.require("chroma_client_auth_credentials_provider")
        self._credentials_provider = system.require(
            get_class(
                str(system.settings.chroma_client_auth_credentials_provider),
                ClientAuthCredentialsProvider,
            )
        )
        _token = self._credentials_provider.get_credentials()
        check_token(_token.get_secret_value())
        if system.settings.chroma_client_auth_token_transport_header:
            self._token_transport_header = TokenTransportHeader[
                str(system.settings.chroma_client_auth_token_transport_header)
            ]

    @override
    def authenticate(self) -> ClientAuthResponse:
        _token = self._credentials_provider.get_credentials()

        return TokenAuthClientAuthResponse(_token, self._token_transport_header)
