import logging

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    ServerAuthProvider,
    ServerAuthenticationResponse,
    ClientAuthProvider,
    ServerAuthCredentialsProvider,
    BasicAuthCredentials,
    AuthHeaders,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)

logger = logging.getLogger(__name__)

__all__ = ["BasicAuthServerProvider", "BasicAuthClientProvider"]


class BasicAuthClientProvider(ClientAuthProvider):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        system.settings.require("chroma_client_auth_credentials")
        self._creds = SecretStr(
            str(system.settings.chroma_client_auth_credentials)
        )

    @override
    def authenticate(self) -> AuthHeaders:
        return {
            "Authorization": SecretStr(
                f"Basic {self._creds.get_secret_value()}"
            ),
        }


class BasicAuthServerProvider(ServerAuthProvider):
    _credentials_provider: ServerAuthCredentialsProvider

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        system.settings.require("chroma_server_auth_credentials_provider")
        self._credentials_provider = system.require(
            self._settings.chroma_server_auth_credentials_provider
        )

    @trace_method("BasicAuthServerProvider.authenticate",
                  OpenTelemetryGranularity.ALL)
    @override
    def authenticate(
        self, headers: AuthHeaders
    ) -> ServerAuthenticationResponse:
        try:
            _auth_header = headers["Authorization"].get_secret_value()
            _validation = self._credentials_provider.validate_credentials(
                BasicAuthCredentials.from_header(_auth_header)
            )
            return ServerAuthenticationResponse(
                _validation,
                self._credentials_provider.get_user_identity(
                    BasicAuthCredentials.from_header(_auth_header)
                ),
            )
        except Exception as e:
            logger.error(
                f"BasicAuthServerProvider.authenticate failed: {repr(e)}"
            )
            return ServerAuthenticationResponse(False, None)
