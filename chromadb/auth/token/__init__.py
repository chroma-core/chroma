import json
import logging
import string
from enum import Enum
from typing import List, Optional, Tuple, Any, TypedDict, cast, Dict, TypeVar

from overrides import override
from pydantic import SecretStr
import yaml

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
    SimpleServerAuthenticationResponse,
    SimpleUserIdentity,
)
from chromadb.auth.registry import register_provider, resolve_provider
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
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

    @trace_method(
        "TokenConfigServerAuthCredentialsProvider.validate_credentials",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def validate_credentials(self, credentials: AbstractCredentials[T]) -> bool:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        if "token" not in _creds:
            logger.error("Returned credentials do not contain token")
            return False
        return _creds["token"].get_secret_value() == self._token.get_secret_value()

    @override
    def get_user_identity(
        self, credentials: AbstractCredentials[T]
    ) -> Optional[SimpleUserIdentity]:
        return None


class Token(TypedDict):
    token: str
    secret: str


class User(TypedDict):
    id: str
    role: str
    tenant: Optional[str]
    tokens: List[Token]


@register_provider("user_token_config")
class UserTokenConfigServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    _users: List[User]
    _token_user_mapping: Dict[str, str]  # reverse mapping of token to user

    def __init__(self, system: System) -> None:
        super().__init__(system)
        if system.settings.chroma_server_auth_credentials_file:
            system.settings.require("chroma_server_auth_credentials_file")
            user_file = str(system.settings.chroma_server_auth_credentials_file)
            with open(user_file) as f:
                self._users = cast(List[User], yaml.safe_load(f)["users"])
        elif system.settings.chroma_server_auth_credentials:
            self._users = cast(
                List[User], json.loads(system.settings.chroma_server_auth_credentials)
            )
        self._token_user_mapping = {}
        for user in self._users:
            for t in user["tokens"]:
                token_str = t["token"]
                check_token(token_str)
                if token_str in self._token_user_mapping:
                    raise ValueError("Token already exists for another user")
                self._token_user_mapping[token_str] = user["id"]

    def find_user_by_id(self, _user_id: str) -> Optional[User]:
        for user in self._users:
            if user["id"] == _user_id:
                return user
        return None

    @override
    def validate_credentials(self, credentials: AbstractCredentials[T]) -> bool:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        if "token" not in _creds:
            logger.error("Returned credentials do not contain token")
            return False
        return _creds["token"].get_secret_value() in self._token_user_mapping.keys()

    @override
    def get_user_identity(
        self, credentials: AbstractCredentials[T]
    ) -> Optional[SimpleUserIdentity]:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        if "token" not in _creds:
            logger.error("Returned credentials do not contain token")
            return None
        # below is just simple identity mapping and may need future work for more
        # complex use cases
        _user_id = self._token_user_mapping[_creds["token"].get_secret_value()]
        _user = self.find_user_by_id(_user_id)
        return SimpleUserIdentity(
            user_id=_user_id,
            tenant=_user["tenant"] if _user and "tenant" in _user else "*",
        )


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

    @trace_method("TokenAuthServerProvider.authenticate", OpenTelemetryGranularity.ALL)
    @override
    def authenticate(
        self, request: ServerAuthenticationRequest[Any]
    ) -> SimpleServerAuthenticationResponse:
        try:
            _auth_header = request.get_auth_info(
                AuthInfoType.HEADER, self._token_transport_header.value
            )
            _token_creds = TokenAuthCredentials.from_header(
                _auth_header, self._token_transport_header
            )
            return SimpleServerAuthenticationResponse(
                self._credentials_provider.validate_credentials(_token_creds),
                self._credentials_provider.get_user_identity(_token_creds),
            )
        except Exception as e:
            logger.error(f"TokenAuthServerProvider.authenticate failed: {repr(e)}")
            return SimpleServerAuthenticationResponse(False, None)


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

    @trace_method("TokenAuthClientProvider.authenticate", OpenTelemetryGranularity.ALL)
    @override
    def authenticate(self) -> ClientAuthResponse:
        _token = self._credentials_provider.get_credentials()

        return TokenAuthClientAuthResponse(_token, self._token_transport_header)
