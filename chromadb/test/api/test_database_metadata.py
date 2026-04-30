"""Tests for database metadata functionality."""

from chromadb.config import DEFAULT_TENANT
from chromadb.test.conftest import ClientFactories


def test_create_database_with_metadata(client_factories: ClientFactories) -> None:
    """Test creating a database with metadata."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    metadata = {"env": "test", "version": 1, "active": True}
    admin_client.create_database("test_db_with_metadata", metadata=metadata)

    db = admin_client.get_database("test_db_with_metadata")
    assert db["name"] == "test_db_with_metadata"
    assert db["metadata"] == metadata


def test_create_database_without_metadata(client_factories: ClientFactories) -> None:
    """Test creating a database without metadata returns None for metadata."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    admin_client.create_database("test_db_no_metadata")

    db = admin_client.get_database("test_db_no_metadata")
    assert db["name"] == "test_db_no_metadata"
    assert db.get("metadata") is None


def test_list_databases_includes_metadata(client_factories: ClientFactories) -> None:
    """Test that list_databases returns metadata for each database."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    admin_client.create_database("db_with_meta", metadata={"type": "primary"})
    admin_client.create_database("db_without_meta")

    databases = admin_client.list_databases()

    db_with_meta = next(d for d in databases if d["name"] == "db_with_meta")
    db_without_meta = next(d for d in databases if d["name"] == "db_without_meta")

    assert db_with_meta["metadata"] == {"type": "primary"}
    assert db_without_meta.get("metadata") is None


def test_database_metadata_with_different_types(
    client_factories: ClientFactories,
) -> None:
    """Test database metadata with various value types."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    metadata = {
        "string_val": "hello",
        "int_val": 42,
        "float_val": 3.14,
        "bool_val": True,
    }
    admin_client.create_database("test_db_types", metadata=metadata)

    db = admin_client.get_database("test_db_types")
    assert db["metadata"]["string_val"] == "hello"
    assert db["metadata"]["int_val"] == 42
    assert db["metadata"]["float_val"] == 3.14
    assert db["metadata"]["bool_val"] is True


def test_database_metadata_in_different_tenants(
    client_factories: ClientFactories,
) -> None:
    """Test database metadata works correctly across different tenants."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    admin_client.create_tenant("tenant_a")
    admin_client.create_tenant("tenant_b")

    admin_client.create_database(
        "shared_name", tenant="tenant_a", metadata={"tenant": "a"}
    )
    admin_client.create_database(
        "shared_name", tenant="tenant_b", metadata={"tenant": "b"}
    )

    db_a = admin_client.get_database("shared_name", tenant="tenant_a")
    db_b = admin_client.get_database("shared_name", tenant="tenant_b")

    assert db_a["metadata"] == {"tenant": "a"}
    assert db_b["metadata"] == {"tenant": "b"}


def test_delete_database_cascades_metadata(client_factories: ClientFactories) -> None:
    """Test that deleting a database also deletes its metadata."""
    client = client_factories.create_client_from_system()
    client.reset()

    admin_client = client_factories.create_admin_client_from_system()

    admin_client.create_database("db_to_delete", metadata={"will_be": "deleted"})

    db = admin_client.get_database("db_to_delete")
    assert db["metadata"] == {"will_be": "deleted"}

    admin_client.delete_database("db_to_delete")

    databases = admin_client.list_databases()
    assert not any(d["name"] == "db_to_delete" for d in databases)
