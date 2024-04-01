import importlib
import logging
from typing import Optional, cast, Dict, TypeVar

from overrides import override
from pydantic import SecretStr
from chromadb.auth import (
    ServerAuthCredentialsProvider,
    AbstractCredentials,
    UserIdentity,
)
from chromadb.auth.registry import register_provider
from chromadb.config import System
from chromadb.telemetry.opentelemetry import (
    OpenTelemetryGranularity,
    trace_method,
)

T = TypeVar("T")

logger = logging.getLogger(__name__)


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

    @trace_method(
        "HtpasswdServerAuthCredentialsProvider.validate_credentials",
        OpenTelemetryGranularity.ALL,
    )
    @override
    def validate_credentials(self, credentials: AbstractCredentials[T]) -> bool:
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


@register_provider("htpasswd_file")
class HtpasswdFileServerAuthCredentialsProvider(HtpasswdServerAuthCredentialsProvider):
    def __init__(self, system: System) -> None:
        super().__init__(system)
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


class HtpasswdConfigurationServerAuthCredentialsProvider(
    HtpasswdServerAuthCredentialsProvider
):
    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_server_auth_credentials")
        _raw_creds = (
            str(system.settings.chroma_server_auth_credentials).strip().split(":")
        )
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
