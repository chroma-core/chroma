import pytest
from unittest.mock import patch
from chromadb import CloudClient
from chromadb.errors import ChromaAuthError
from chromadb.auth import UserIdentity
from chromadb.types import Tenant, Database
from uuid import uuid4


def test_valid_key() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database, patch(
        "chromadb.api.fastapi.FastAPI.heartbeat"
    ) as mock_heartbeat:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="default_tenant", databases=["testdb"]
        )
        mock_get_tenant.return_value = Tenant(name="default_tenant")
        mock_get_database.return_value = Database(
            id=uuid4(), name="testdb", tenant="default_tenant"
        )
        mock_heartbeat.return_value = 1234567890

        client = CloudClient(database="testdb", api_key="valid_token")

        assert client.get_user_identity().user_id == "test_user"
        assert client.get_user_identity().tenant == "default_tenant"
        assert client.get_user_identity().databases == ["testdb"]

        settings = client.get_settings()
        assert settings.chroma_client_auth_credentials == "valid_token"
        assert (
            settings.chroma_client_auth_provider
            == "chromadb.auth.token_authn.TokenAuthClientProvider"
        )

        assert client.heartbeat() == 1234567890


def test_invalid_key() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.side_effect = ChromaAuthError("Authentication failed")

        with pytest.raises(ChromaAuthError):
            CloudClient(database="testdb", api_key="invalid_token")
