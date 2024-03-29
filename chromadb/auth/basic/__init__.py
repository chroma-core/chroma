import base64
import logging
from typing import Tuple, Any, cast

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    ServerAuthProvider,
    ClientAuthProvider,
    ServerAuthenticationRequest,
    ServerAuthCredentialsProvider,
    AuthInfoType,
    BasicAuthCredentials,
    ClientAuthCredentialsProvider,
    ClientAuthResponse,
    SimpleServerAuthenticationResponse,
)
from chromadb.auth.registry import register_provider, resolve_provider
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from chromadb.utils import get_class

logger = logging.getLogger(__name__)

__all__ = ["BasicAuthServerProvider", "BasicAuthClientProvider"]


class BasicAuthClientAuthResponse(ClientAuthResponse):
    def __init__(self, credentials: SecretStr) -> None:
        self._credentials = credentials

    @override
    def get_auth_info_type(self) -> AuthInfoType:
        return AuthInfoType.HEADER

    @override
    def get_auth_info(self) -> Tuple[str, SecretStr]:
        return "Authorization", SecretStr(
            f"Basic {self._credentials.get_secret_value()}"
        )


@register_provider("basic")
class BasicAuthClientProvider(ClientAuthProvider):
    _credentials_provider: ClientAuthCredentialsProvider[Any]

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

    @override
    def authenticate(self) -> ClientAuthResponse:
        _creds = self._credentials_provider.get_credentials()
        return BasicAuthClientAuthResponse(
            SecretStr(
                base64.b64encode(f"{_creds.get_secret_value()}".encode("utf-8")).decode(
                    "utf-8"
                )
            )
        )


@register_provider("basic")
class BasicAuthServerProvider(ServerAuthProvider):
    _credentials_provider: ServerAuthCredentialsProvider

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

    @trace_method("BasicAuthServerProvider.authenticate", OpenTelemetryGranularity.ALL)
    @override
    async def authenticate(
        self, request: ServerAuthenticationRequest[Any]
    ) -> SimpleServerAuthenticationResponse:
        try:
            _auth_header = request.get_auth_info(AuthInfoType.HEADER, "Authorization")
            _validation = await self._credentials_provider.validate_credentials(
                BasicAuthCredentials.from_header(_auth_header)
            )
            return SimpleServerAuthenticationResponse(
                _validation,
                await self._credentials_provider.get_user_identity(
                    BasicAuthCredentials.from_header(_auth_header)
                ),
            )
        except Exception as e:
            logger.error(f"BasicAuthServerProvider.authenticate failed: {repr(e)}")
            return SimpleServerAuthenticationResponse(False, None)
