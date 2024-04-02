import base64
import importlib
import logging

from overrides import override
from pydantic import SecretStr

from chromadb.auth import (
    UserIdentity,
    ServerAuthenticationProvider,
    ServerAuthenticationResponse,
    ClientAuthProvider,
    ClientAuthHeaders,
)
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)
from starlette.datastructures import Headers

logger = logging.getLogger(__name__)

__all__ = ["BasicAuthenticationServerProvider", "BasicAuthClientProvider"]


class BasicAuthClientProvider(ClientAuthProvider):
    """
    Client auth provider for basic auth. The credentials are passed as a
    base64-encoded string in the Authorization header prepended with "Basic ".
    """
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


class BasicAuthenticationServerProvider(ServerAuthenticationProvider):
    """
    Server auth provider for basic auth. The credentials are read from
    `chroma_server_authn_credentials_file` and each line must be in the format
    <username>:<bcrypt passwd>.

    Expects tokens to be passed as a base64-encoded string in the Authorization
    header prepended with "Basic".
    """
    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings

        try:
            # We need this to check passwords
            self.bc = importlib.import_module("bcrypt")
        except ImportError:
            raise ValueError(
                "The bcrypt python package is not installed. "
                "Please install it with `pip install bcrypt`"
            )

        self._creds = {}

        system.settings.require("chroma_server_authn_credentials_file")
        _creds_file = str(system.settings.chroma_server_authn_credentials_file)
        with open(_creds_file, "r") as f:
            for line in f:
                _raw_creds = [v for v in line.strip().split(":")]
                if len(_raw_creds) != 2 or not all(_raw_creds):
                    raise ValueError(
                        "Invalid Htpasswd credentials found in "
                        "[chroma_server_auth_credentials]. "
                        "Lines must be exactly <username>:<bcrypt passwd>."
                    )
                username = _raw_creds[0]
                password = _raw_creds[1]
                if username in self._creds:
                    raise ValueError(
                        "Duplicate username found in "
                        "[chroma_server_auth_credentials]. "
                        "Usernames must be unique."
                    )
                self._creds[username] = SecretStr(password)

    @trace_method("BasicAuthenticationServerProvider.authenticate",
                  OpenTelemetryGranularity.ALL)
    @override
    def authenticate(
        self, headers: Headers
    ) -> ServerAuthenticationResponse:
        try:
            _auth_header = headers["Authorization"]
            _auth_header = _auth_header.replace("Basic ", "")
            _auth_header = _auth_header.strip()

            base64_decoded = base64.b64decode(_auth_header).decode("us-ascii")
            username, password = base64_decoded.split(":")

            _usr_check = bool(
                username
                == self._creds["username"].get_secret_value()
            )
            _pwd_check = self.bc.checkpw(
                password.encode("utf-8"),
                self._creds["password"].get_secret_value().encode("utf-8"),
            )
            success = _usr_check and _pwd_check
            return ServerAuthenticationResponse(
                success,
                UserIdentity(user_id=username) if success else None,
            )
        except Exception as e:
            logger.error(
                "BasicAuthenticationServerProvider.authenticate "
                f"failed: {repr(e)}"
            )
            return ServerAuthenticationResponse(False, None)
