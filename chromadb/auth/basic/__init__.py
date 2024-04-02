import logging

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    ServerAuthProvider,
    ServerAuthenticationResponse,
    ClientAuthProvider,
    ServerAuthCredentialsProvider,
    ClientAuthHeaders,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from starlette.datastructures import Headers

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
    def authenticate(self) -> ClientAuthHeaders:
        return {
            "Authorization": SecretStr(
                f"Basic {self._creds.get_secret_value()}"
            ),
        }


class BasicAuthCredentials:
    def __init__(self, username: SecretStr, password: SecretStr) -> None:
        self.username = username
        self.password = password

    @override
    def get_credentials(self) -> Dict[str, SecretStr]:
        return {"username": self.username, "password": self.password}

    @staticmethod
    def from_header(header: str) -> BasicAuthCredentials:
        """
        Parses a basic auth header and returns a BasicAuthCredentials object.
        """
        header = header.replace("Basic ", "")
        header = header.strip()
        base64_decoded = base64.b64decode(header).decode("utf-8")
        username, password = base64_decoded.split(":")
        return BasicAuthCredentials(SecretStr(username), SecretStr(password))


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
        self, headers: Headers
    ) -> ServerAuthenticationResponse:
        try:
            _auth_header = headers["Authorization"]
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


class HtpasswdServerAuthCredentialsProvider(ServerAuthCredentialsProvider):
    _creds: Dict[str, SecretStr]

    def __init__(self, system: System) -> None:
        super().__init__(system)
        try:
            # Equivalent to import onnxruntime
            self.bc = importlib.import_module("bcrypt")
        except ImportError:
            raise ValueError(
                "The bcrypt python package is not installed. "
                "Please install it with `pip install bcrypt`"
            )
        system.settings.require("chroma_server_auth_credentials_file")
        _file = str(system.settings.chroma_server_auth_credentials_file)
        with open(_file, "r") as f:
            _raw_creds = [v for v in f.readline().strip().split(":")]
            self._creds = {
                "username": SecretStr(_raw_creds[0]),
                "password": SecretStr(_raw_creds[1]),
            }
        if (
            len(self._creds) != 2
            or "username" not in self._creds
            or "password" not in self._creds
        ):
            raise ValueError(
                "Invalid Htpasswd credentials found in "
                "[chroma_server_auth_credentials]. "
                "Must be <username>:<bcrypt passwd>."
            )

    @trace_method(
        "HtpasswdServerAuthCredentialsProvider.validate_credentials",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def validate_credentials(self,
                             credentials: AbstractCredentials[T]) -> bool:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        if len(_creds) != 2:
            logger.error(
                "Returned credentials did match expected format: "
                "dict[username:SecretStr, password: SecretStr]"
            )
            return False
        if "username" not in _creds or "password" not in _creds:
            logger.error(
                "Returned credentials do not contain username or password")
            return False
        _usr_check = bool(
            _creds["username"].get_secret_value()
            == self._creds["username"].get_secret_value()
        )
        return _usr_check and self.bc.checkpw(
            _creds["password"].get_secret_value().encode("utf-8"),
            self._creds["password"].get_secret_value().encode("utf-8"),
        )

    @override
    def get_user_identity(
        self, credentials: AbstractCredentials[T]
    ) -> Optional[UserIdentity]:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        return UserIdentity(user_id=_creds["username"].get_secret_value())
