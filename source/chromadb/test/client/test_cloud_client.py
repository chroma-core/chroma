import pytest
from unittest.mock import patch
from chromadb import CloudClient
from chromadb.errors import ChromaAuthError, NotFoundError
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


# Scoped API key to 1 database tests
def test_scoped_api_key_to_single_db_with_api_key_only() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        # mock single db scoped api key
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.return_value = Database(
            id=uuid4(), name="right-db", tenant="123-456-789"
        )

        client = CloudClient(api_key="valid_token")

        # should resolve to single db
        assert client.database == "right-db"
        assert client.tenant == "123-456-789"


def test_scoped_api_key_to_single_db_with_correct_tenant() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.return_value = Database(
            id=uuid4(), name="right-db", tenant="123-456-789"
        )

        client = CloudClient(tenant="123-456-789", api_key="valid_token")

        assert client.tenant == "123-456-789"
        assert client.database == "right-db"


def test_scoped_api_key_to_single_db_with_correct_db() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.return_value = Database(
            id=uuid4(), name="right-db", tenant="123-456-789"
        )

        client = CloudClient(database="right-db", api_key="valid_token")

        assert client.tenant == "123-456-789"
        assert client.database == "right-db"


def test_scoped_api_key_to_single_db_with_correct_tenant_and_db() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.return_value = Database(
            id=uuid4(), name="right-db", tenant="123-456-789"
        )

        client = CloudClient(
            tenant="123-456-789", database="right-db", api_key="valid_token"
        )

        assert client.tenant == "123-456-789"
        assert client.database == "right-db"


def test_scoped_api_key_to_single_db_with_wrong_tenant() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Tenant wrong-tenant does not match 123-456-789 from the server. Are you sure the tenant is correct?",
        ):
            CloudClient(tenant="wrong-tenant", api_key="valid_token")


def test_scoped_api_key_to_single_db_with_wrong_database() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["right-db"]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Database wrong-db does not match right-db from the server. Are you sure the database is correct?",
        ):
            CloudClient(database="wrong-db", api_key="valid_token")


def test_scoped_api_key_to_single_db_with_wrong_api_key() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.side_effect = ChromaAuthError("Permission denied.")

        with pytest.raises(ChromaAuthError, match="Permission denied."):
            CloudClient(database="right-db", api_key="wrong-api-key")


# Scoped API key to multiple databases tests
def test_scoped_api_key_to_multiple_dbs_with_wrong_tenant() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user",
            tenant="123-456-789",
            databases=["right-db", "another-db"],
        )

        with pytest.raises(
            ChromaAuthError,
            match="Tenant wrong-tenant does not match 123-456-789 from the server. Are you sure the tenant is correct?",
        ):
            CloudClient(
                tenant="wrong-tenant", database="right-db", api_key="valid_token"
            )


def test_scoped_api_key_to_multiple_dbs_with_correct_tenant_and_db() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user",
            tenant="123-456-789",
            databases=["right-db", "another-db"],
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.return_value = Database(
            id=uuid4(), name="right-db", tenant="123-456-789"
        )

        client = CloudClient(
            tenant="123-456-789", database="right-db", api_key="valid_token"
        )

        assert client.tenant == "123-456-789"
        assert client.database == "right-db"


def test_scoped_api_key_to_multiple_dbs_with_nonexistent_database() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity, patch(
        "chromadb.api.client.AdminClient.get_tenant"
    ) as mock_get_tenant, patch(
        "chromadb.api.client.AdminClient.get_database"
    ) as mock_get_database:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user",
            tenant="123-456-789",
            databases=["right-db", "another-db"],
        )
        mock_get_tenant.return_value = Tenant(name="123-456-789")
        mock_get_database.side_effect = NotFoundError(
            "Database [wrong-db] not found. Are you sure it exists?"
        )

        with pytest.raises(
            NotFoundError,
            match="Database \\[wrong-db\\] not found. Are you sure it exists?",
        ):
            CloudClient(database="wrong-db", api_key="valid_token")


def test_scoped_api_key_to_multiple_dbs_with_api_key_only() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user",
            tenant="123-456-789",
            databases=["right-db", "another-db"],
        )

        with pytest.raises(
            ChromaAuthError,
            match="Could not determine a database name from the current authentication method. Please provide a database name.",
        ):
            CloudClient(api_key="valid_token")


# Unscoped API key tests
def test_api_key_with_unscoped_tenant() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="*", databases=["right-db"]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Could not determine a tenant from the current authentication method. Please provide a tenant.",
        ):
            CloudClient(api_key="valid_token")


def test_api_key_with_unscoped_db() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=["*"]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Could not determine a database name from the current authentication method. Please provide a database name.",
        ):
            CloudClient(api_key="valid_token")


def test_api_key_with_no_db_access() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant="123-456-789", databases=[]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Could not determine a database name from the current authentication method. Please provide a database name.",
        ):
            CloudClient(api_key="valid_token")


def test_api_key_with_no_tenant_access() -> None:
    with patch(
        "chromadb.api.fastapi.FastAPI.get_user_identity"
    ) as mock_get_user_identity:
        mock_get_user_identity.return_value = UserIdentity(
            user_id="test_user", tenant=None, databases=["right-db"]
        )

        with pytest.raises(
            ChromaAuthError,
            match="Could not determine a tenant from the current authentication method. Please provide a tenant.",
        ):
            CloudClient(api_key="valid_token")
