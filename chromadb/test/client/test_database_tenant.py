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


def test_database_collections_add(client: Client) -> None:
    client.reset()

    # Create a new database in the default tenant
    admin_client = AdminClient.from_system(client._system)
    admin_client.create_database("test_db")

    # Create collections in this new database
    client.set_database(database="test_db")
    coll_new = client.create_collection("collection_new")

    # Create collections in the default database
    client.set_database(database=DEFAULT_DATABASE)
    coll_default = client.create_collection("collection_default")

    records_new = {
        "ids": ["a", "b", "c"],
        "embeddings": [[1.0, 2.0, 3.0] for _ in range(3)],
        "documents": ["a", "b", "c"],
    }

    records_default = {
        "ids": ["c", "d", "e"],
        "embeddings": [[4.0, 5.0, 6.0] for _ in range(3)],
        "documents": ["c", "d", "e"],
    }

    # Add to the new coll
    coll_new.add(**records_new)  # type: ignore

    # Add to the default coll
    coll_default.add(**records_default)  # type: ignore

    # Make sure the collections are isolated
    res = coll_new.get(include=["embeddings", "documents"])
    assert res["ids"] == records_new["ids"]
    assert res["embeddings"] == records_new["embeddings"]
    assert res["documents"] == records_new["documents"]

    res = coll_default.get(include=["embeddings", "documents"])
    assert res["ids"] == records_default["ids"]
    assert res["embeddings"] == records_default["embeddings"]
    assert res["documents"] == records_default["documents"]


def test_tenant_collections_add(client: Client) -> None:
    client.reset()

    # Create two databases with same name in different tenants
    admin_client = AdminClient.from_system(client._system)
    admin_client.create_tenant("test_tenant1")
    admin_client.create_tenant("test_tenant2")
    admin_client.create_database("test_db", tenant="test_tenant1")
    admin_client.create_database("test_db", tenant="test_tenant2")

    # Create collections in each database with same name
    client.set_tenant(tenant="test_tenant1", database="test_db")
    coll_tenant1 = client.create_collection("collection")
    client.set_tenant(tenant="test_tenant2", database="test_db")
    coll_tenant2 = client.create_collection("collection")

    records_tenant1 = {
        "ids": ["a", "b", "c"],
        "embeddings": [[1.0, 2.0, 3.0] for _ in range(3)],
        "documents": ["a", "b", "c"],
    }

    records_tenant2 = {
        "ids": ["c", "d", "e"],
        "embeddings": [[4.0, 5.0, 6.0] for _ in range(3)],
        "documents": ["c", "d", "e"],
    }

    # Add to the tenant1 coll
    coll_tenant1.add(**records_tenant1)  # type: ignore

    # Add to the tenant2 coll
    coll_tenant2.add(**records_tenant2)  # type: ignore

    # Make sure the collections are isolated
    res = coll_tenant1.get(include=["embeddings", "documents"])
    assert res["ids"] == records_tenant1["ids"]
    assert res["embeddings"] == records_tenant1["embeddings"]
    assert res["documents"] == records_tenant1["documents"]

    res = coll_tenant2.get(include=["embeddings", "documents"])
    assert res["ids"] == records_tenant2["ids"]
    assert res["embeddings"] == records_tenant2["embeddings"]
    assert res["documents"] == records_tenant2["documents"]


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
