import base64
import os

import requests
from overrides import overrides
from pydantic import SecretStr

from chromadb.auth import ServerAuthProvider


def _encode_credentials(username: str, password: str) -> SecretStr:
    return SecretStr(
        base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    )


class BasicAuthClientProvider(ClientAuthProvider):
    _basic_auth_token: SecretStr

    def __init__(self, settings: "Settings") -> None:
        super().__init__(settings)
        self._settings = settings
        if os.environ.get("CHROMA_CLIENT_AUTH_BASIC_USERNAME") and os.environ.get(
            "CHROMA_CLIENT_AUTH_BASIC_PASSWORD"
        ):
            self._basic_auth_token = _encode_credentials(
                os.environ.get("CHROMA_CLIENT_AUTH_BASIC_USERNAME", ""),
                os.environ.get("CHROMA_CLIENT_AUTH_BASIC_PASSWORD", ""),
            )
        elif isinstance(
            self._settings.chroma_client_auth_provider_config, str
        ) and os.path.exists(self._settings.chroma_client_auth_provider_config):
            with open(self._settings.chroma_client_auth_provider_config) as f:
                # read first line of file which should be user:password
                _auth_data = f.readline().strip().split(":")
                # validate auth data
                if len(_auth_data) != 2:
                    raise ValueError("Invalid auth data")
                self._basic_auth_token = _encode_credentials(
                    _auth_data[0], _auth_data[1]
                )
        elif self._settings.chroma_client_auth_provider_config and isinstance(
            self._settings.chroma_client_auth_provider_config, dict
        ):
            self._basic_auth_token = _encode_credentials(
                self._settings.chroma_client_auth_provider_config["username"],
                self._settings.chroma_client_auth_provider_config["password"],
            )
        else:
            raise ValueError("Basic auth credentials not found")

    @overrides
    def authenticate(self, session: requests.Session) -> None:
        session.headers.update(
            {"Authorization": f"Basic {self._basic_auth_token.get_secret_value()}"}
        )


class BasicAuthServerProvider(ServerAuthProvider):
    _basic_auth_token: SecretStr

    def __init__(self, settings: "Settings") -> None:
        super().__init__(settings)
        self._settings = settings
        self._basic_auth_token = SecretStr("")
        if os.environ.get("CHROMA_SERVER_AUTH_BASIC_USERNAME") and os.environ.get(
            "CHROMA_SERVER_AUTH_BASIC_PASSWORD"
        ):
            self._basic_auth_token = _encode_credentials(
                os.environ.get("CHROMA_SERVER_AUTH_BASIC_USERNAME", ""),
                os.environ.get("CHROMA_SERVER_AUTH_BASIC_PASSWORD", ""),
            )
            self._ignore_auth_paths = os.environ.get(
                "CHROMA_SERVER_AUTH_IGNORE_PATHS", ",".join(self._ignore_auth_paths)
            ).split(",")
        elif isinstance(
            self._settings.chroma_server_auth_provider_config, str
        ) and os.path.exists(self._settings.chroma_server_auth_provider_config):
            with open(self._settings.chroma_server_auth_provider_config) as f:
                # read first line of file which should be user:password
                _auth_data = f.readline().strip().split(":")
                # validate auth data
                if len(_auth_data) != 2:
                    raise ValueError("Invalid auth data")
                self._basic_auth_token = _create_token(_auth_data[0], _auth_data[1])
            self._ignore_auth_paths = os.environ.get(
                "CHROMA_SERVER_AUTH_IGNORE_PATHS", ",".join(self._ignore_auth_paths)
            ).split(",")
        elif self._settings.chroma_server_auth_provider_config and isinstance(
            self._settings.chroma_server_auth_provider_config, dict
        ):
            # encode the username and password base64
            self._basic_auth_token = _create_token(
                self._settings.chroma_server_auth_provider_config["username"],
                self._settings.chroma_server_auth_provider_config["password"],
            )
            if "ignore_auth_paths" in self._settings.chroma_server_auth_provider_config:
                self._ignore_auth_paths = (
                    self._settings.chroma_server_auth_provider_config[
                        "ignore_auth_paths"
                    ]
                )
        else:
            raise ValueError("Basic auth credentials not found")

    @overrides
    def authenticate(self, request: Request) -> Union[Response, None]:
        auth_header = request.headers.get("Authorization", "").split()
        # Check if the header exists and the token is correct
        if request.url.path in self._ignore_auth_paths:
            logger.debug(f"Skipping auth for path {request.url.path}")
            return None
        if (
            len(auth_header) != 2
            or auth_header[1] != self._basic_auth_token.get_secret_value()
        ):
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
        return None
