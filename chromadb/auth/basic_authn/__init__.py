import base64

from overrides import override
from pydantic import SecretStr

from chromadb.auth import ClientAuthHeaders, ClientAuthProvider
from chromadb.config import System

__all__ = ["BasicAuthClientProvider"]

AUTHORIZATION_HEADER = "Authorization"


class BasicAuthClientProvider(ClientAuthProvider):
    """
    Client auth provider for basic auth. The credentials are passed as a
    base64-encoded string in the Authorization header prepended with "Basic ".
    """

    def __init__(self, system: System) -> None:
        super().__init__(system)
        self._settings = system.settings
        system.settings.require("chroma_client_auth_credentials")
        self._creds = SecretStr(str(system.settings.chroma_client_auth_credentials))

    @override
    def authenticate(self) -> ClientAuthHeaders:
        encoded = base64.b64encode(
            f"{self._creds.get_secret_value()}".encode("utf-8")
        ).decode("utf-8")
        return {
            AUTHORIZATION_HEADER: SecretStr(f"Basic {encoded}"),
        }
