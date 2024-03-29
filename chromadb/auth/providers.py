import importlib
import logging
from typing import Optional, cast, Dict, TypeVar, Any

import requests
from overrides import override
from pydantic import SecretStr
from chromadb.auth import (
    ServerAuthCredentialsProvider,
    AbstractCredentials,
    ClientAuthCredentialsProvider,
    AuthInfoType,
    ClientAuthProvider,
    ClientAuthProtocolAdapter,
    SimpleUserIdentity,
)
from chromadb.auth.registry import register_provider, resolve_provider
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
    ) -> Optional[SimpleUserIdentity]:
        _creds = cast(Dict[str, SecretStr], credentials.get_credentials())
        return SimpleUserIdentity(_creds["username"].get_secret_value())


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


class RequestsClientAuthProtocolAdapter(
    ClientAuthProtocolAdapter[requests.PreparedRequest]
):
    class _Session(requests.Session):
        _protocol_adapter: ClientAuthProtocolAdapter[requests.PreparedRequest]

        def __init__(
            self, protocol_adapter: ClientAuthProtocolAdapter[requests.PreparedRequest]
        ) -> None:
            super().__init__()
            self._protocol_adapter = protocol_adapter

        @override
        def send(
            self, request: requests.PreparedRequest, **kwargs: Any
        ) -> requests.Response:
            self._protocol_adapter.inject_credentials(request)
            return super().send(request, **kwargs)

    _session: _Session
    _auth_provider: ClientAuthProvider

    def __init__(self, system: System) -> None:
        super().__init__(system)
        system.settings.require("chroma_client_auth_provider")
        self._auth_provider = cast(
            ClientAuthProvider,
            system.require(
                resolve_provider(
                    str(system.settings.chroma_client_auth_provider), ClientAuthProvider
                ),
            ),
        )
        self._session = self._Session(self)
        self._auth_header = self._auth_provider.authenticate()

    @property
    def session(self) -> requests.Session:
        return self._session

    @override
    def inject_credentials(self, injection_context: requests.PreparedRequest) -> None:
        if self._auth_header.get_auth_info_type() == AuthInfoType.HEADER:
            _header_info = self._auth_header.get_auth_info()
            if isinstance(_header_info, tuple):
                injection_context.headers[_header_info[0]] = _header_info[
                    1
                ].get_secret_value()
            else:
                for header in _header_info:
                    injection_context.headers[header[0]
                                              ] = header[1].get_secret_value()
        else:
            raise ValueError(
                f"Unsupported auth type: {self._auth_header.get_auth_info_type()}"
            )
