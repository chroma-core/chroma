import pytest
from chromadb.api.client import AdminClient, Client
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT


def test_database_tenant_collections(client: Client) -> None:
    client.reset()
    # Create a new database in the default tenant
    admin_client = AdminClient.from_system(client._system)
    admin_client.create_database("test_db")

    # Create collections in this new database
    client.set_tenant(tenant=DEFAULT_TENANT, database="test_db")
    client.create_collection("collection", metadata={"database": "test_db"})

    # Create collections in the default database
    client.set_tenant(tenant=DEFAULT_TENANT, database=DEFAULT_DATABASE)
    client.create_collection("collection", metadata={"database": DEFAULT_DATABASE})

    # List collections in the default database
    collections = client.list_collections()
    assert len(collections) == 1
    assert collections[0].name == "collection"
    assert collections[0].metadata == {"database": DEFAULT_DATABASE}

    # List collections in the new database
    client.set_tenant(tenant=DEFAULT_TENANT, database="test_db")
    collections = client.list_collections()
    assert len(collections) == 1
    assert collections[0].metadata == {"database": "test_db"}

    # Update the metadata in both databases to different values
    client.set_tenant(tenant=DEFAULT_TENANT, database=DEFAULT_DATABASE)
    client.list_collections()[0].modify(metadata={"database": "default2"})

    client.set_tenant(tenant=DEFAULT_TENANT, database="test_db")
    client.list_collections()[0].modify(metadata={"database": "test_db2"})

    # Validate that the metadata was updated
    client.set_tenant(tenant=DEFAULT_TENANT, database=DEFAULT_DATABASE)
    collections = client.list_collections()
    assert len(collections) == 1
    assert collections[0].metadata == {"database": "default2"}

    client.set_tenant(tenant=DEFAULT_TENANT, database="test_db")
    collections = client.list_collections()
    assert len(collections) == 1
    assert collections[0].metadata == {"database": "test_db2"}

    # Delete the collections and make sure databases are isolated
    client.set_tenant(tenant=DEFAULT_TENANT, database=DEFAULT_DATABASE)
    client.delete_collection("collection")

    collections = client.list_collections()
    assert len(collections) == 0

    client.set_tenant(tenant=DEFAULT_TENANT, database="test_db")
    collections = client.list_collections()
    assert len(collections) == 1

    client.delete_collection("collection")
    collections = client.list_collections()
    assert len(collections) == 0


def test_min_len_name(client: Client) -> None:
    client.reset()

    # Create a new database in the default tenant with a name of length 1
    # and expect an error
    admin_client = AdminClient.from_system(client._system)
    with pytest.raises(Exception):
        admin_client.create_database("a")

    # Create a tenant with a name of length 1 and expect an error
    with pytest.raises(Exception):
        admin_client.create_tenant("a")
