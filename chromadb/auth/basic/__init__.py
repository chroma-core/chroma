import logging
from typing import cast

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    ServerAuthProvider,
    ClientAuthProvider,
    ServerAuthCredentialsProvider,
    BasicAuthCredentials,
    ClientAuthHeaders,
)
from chromadb.auth.registry import register_provider, resolve_provider
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)

logger = logging.getLogger(__name__)

__all__ = ["BasicAuthServerProvider", "BasicAuthClientProvider"]


@register_provider("basic")
class BasicAuthClientProvider(ClientAuthProvider):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        system.settings.require("chroma_client_auth_credentials")
        self._creds = SecretStr(
            str(system.settings.chroma_client_auth_credentials)
        )

    @override
    def authenticate(self) -> ClientAuthHeaders:
        return {
            "Authorization": SecretStr(f"Basic {self._creds.get_secret_value()}"),
        }


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
    def authenticate(
        self, request: ServerAuthenticationRequest[Any]
    ) -> SimpleServerAuthenticationResponse:
        try:
            _auth_header = request.get_auth_info("Authorization")
            _validation = self._credentials_provider.validate_credentials(
                BasicAuthCredentials.from_header(_auth_header)
            )
            return SimpleServerAuthenticationResponse(
                _validation,
                self._credentials_provider.get_user_identity(
                    BasicAuthCredentials.from_header(_auth_header)
                ),
            )
        except Exception as e:
            logger.error(f"BasicAuthServerProvider.authenticate failed: {repr(e)}")
            return SimpleServerAuthenticationResponse(False, None)
