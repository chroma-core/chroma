import os
import shutil
import tempfile
import pytest
from typing import Generator, List, Callable, Dict, Union

from chromadb.db.impl.grpc.client import GrpcSysDB
from chromadb.db.impl.grpc.server import GrpcMockSysDB
from chromadb.types import Collection, Segment, SegmentScope
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import (
    DEFAULT_DATABASE,
    DEFAULT_TENANT,
    Component,
    System,
    Settings,
)
from chromadb.db.system import SysDB
from chromadb.db.base import NotFoundError, UniqueConstraintError
from pytest import FixtureRequest
import uuid

# These are the sample collections that are used in the tests below. Tests can override
# the fields as needed.
sample_collections = [
    Collection(
        id=uuid.UUID(int=1),
        name="test_collection_1",
        topic="persistent://test-tenant/test-topic/00000000-0000-0000-0000-000000000001",
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
        dimension=128,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
    ),
    Collection(
        id=uuid.UUID(int=2),
        name="test_collection_2",
        topic="persistent://test-tenant/test-topic/00000000-0000-0000-0000-000000000002",
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
        dimension=None,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
    ),
    Collection(
        id=uuid.UUID(int=3),
        name="test_collection_3",
        topic="persistent://test-tenant/test-topic/00000000-0000-0000-0000-000000000003",
        metadata={"test_str": "str3", "test_int": 3, "test_float": 3.3},
        dimension=None,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
    ),
]


class MockAssignmentPolicy(Component):
    def assign_collection(self, collection_id: uuid.UUID) -> str:
        for collection in sample_collections:
            if collection["id"] == collection_id:
                return collection["topic"]
        raise ValueError(f"Unknown collection ID: {collection_id}")


def sqlite() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    db = SqliteDB(
        System(
            Settings(
                allow_reset=True,
                chroma_collection_assignment_policy_impl="chromadb.test.db.test_system.MockAssignmentPolicy",
            )
        )
    )
    db.start()
    yield db
    db.stop()


def sqlite_persistent() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    save_path = tempfile.mkdtemp()
    db = SqliteDB(
        System(
            Settings(
                allow_reset=True,
                is_persistent=True,
                persist_directory=save_path,
                chroma_collection_assignment_policy_impl="chromadb.test.db.test_system.MockAssignmentPolicy",
            )
        )
    )
    db.start()
    yield db
    db.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def grpc_with_mock_server() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB that creates a mock grpc sysdb server
    and a grpc client that connects to it."""
    system = System(
        Settings(
            allow_reset=True,
            chroma_collection_assignment_policy_impl="chromadb.test.db.test_system.MockAssignmentPolicy",
            chroma_server_grpc_port=50051,
        )
    )
    system.instance(GrpcMockSysDB)
    client = system.instance(GrpcSysDB)
    system.start()
    client.reset_and_wait_for_ready()
    yield client


def grpc_with_real_server() -> Generator[SysDB, None, None]:
    system = System(
        Settings(
            allow_reset=True,
            chroma_collection_assignment_policy_impl="chromadb.test.db.test_system.MockAssignmentPolicy",
        )
    )
    client = system.instance(GrpcSysDB)
    system.start()
    client.reset_and_wait_for_ready()
    yield client


def db_fixtures() -> List[Callable[[], Generator[SysDB, None, None]]]:
    if "CHROMA_CLUSTER_TEST_ONLY" in os.environ:
        return [grpc_with_real_server]
    else:
        return [sqlite, sqlite_persistent, grpc_with_mock_server]


@pytest.fixture(scope="module", params=db_fixtures())
def sysdb(request: FixtureRequest) -> Generator[SysDB, None, None]:
    yield next(request.param())


# region Collection tests
def test_create_get_delete_collections(sysdb: SysDB) -> None:
    sysdb.reset_state()

    for collection in sample_collections:
        sysdb.create_collection(
            id=collection["id"],
            name=collection["name"],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
        )
        collection["database"] = DEFAULT_DATABASE
        collection["tenant"] = DEFAULT_TENANT

    results = sysdb.get_collections()
    results = sorted(results, key=lambda c: c["name"])

    assert sorted(results, key=lambda c: c["name"]) == sample_collections

    # Duplicate create fails
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            name=sample_collections[0]["name"], id=sample_collections[0]["id"]
        )

    # Find by name
    for collection in sample_collections:
        result = sysdb.get_collections(name=collection["name"])
        assert result == [collection]

    # Find by topic
    for collection in sample_collections:
        result = sysdb.get_collections(topic=collection["topic"])
        assert result == [collection]

    # Find by id
    for collection in sample_collections:
        result = sysdb.get_collections(id=collection["id"])
        assert result == [collection]

    # Find by id and topic (positive case)
    for collection in sample_collections:
        result = sysdb.get_collections(id=collection["id"], topic=collection["topic"])
        assert result == [collection]

    # find by id and topic (negative case)
    for collection in sample_collections:
        result = sysdb.get_collections(id=collection["id"], topic="other_topic")
        assert result == []

    # Delete
    c1 = sample_collections[0]
    sysdb.delete_collection(c1["id"])

    results = sysdb.get_collections()
    assert c1 not in results
    assert len(results) == len(sample_collections) - 1
    assert sorted(results, key=lambda c: c["name"]) == sample_collections[1:]

    by_id_result = sysdb.get_collections(id=c1["id"])
    assert by_id_result == []

    # Duplicate delete throws an exception
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(c1["id"])


def test_update_collections(sysdb: SysDB) -> None:
    coll = Collection(
        name=sample_collections[0]["name"],
        id=sample_collections[0]["id"],
        topic=sample_collections[0]["topic"],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
    )

    sysdb.reset_state()

    sysdb.create_collection(
        id=coll["id"],
        name=coll["name"],
        metadata=coll["metadata"],
        dimension=coll["dimension"],
    )

    # Update name
    coll["name"] = "new_name"
    sysdb.update_collection(coll["id"], name=coll["name"])
    result = sysdb.get_collections(name=coll["name"])
    assert result == [coll]

    # Update topic
    coll["topic"] = "new_topic"
    sysdb.update_collection(coll["id"], topic=coll["topic"])
    result = sysdb.get_collections(topic=coll["topic"])
    assert result == [coll]

    # Update dimension
    coll["dimension"] = 128
    sysdb.update_collection(coll["id"], dimension=coll["dimension"])
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]

    # Reset the metadata
    coll["metadata"] = {"test_str2": "str2"}
    sysdb.update_collection(coll["id"], metadata=coll["metadata"])
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]

    # Delete all metadata keys
    coll["metadata"] = None
    sysdb.update_collection(coll["id"], metadata=None)
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]


def test_get_or_create_collection(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # get_or_create = True returns existing collection
    collection = sample_collections[0]

    sysdb.create_collection(
        id=collection["id"],
        name=collection["name"],
        metadata=collection["metadata"],
        dimension=collection["dimension"],
    )

    result, created = sysdb.create_collection(
        name=collection["name"],
        id=uuid.uuid4(),
        get_or_create=True,
        metadata=collection["metadata"],
    )
    assert result == collection

    # Only one collection with the same name exists
    get_result = sysdb.get_collections(name=collection["name"])
    assert get_result == [collection]

    # get_or_create = True creates new collection
    result, created = sysdb.create_collection(
        name=sample_collections[1]["name"],
        id=sample_collections[1]["id"],
        get_or_create=True,
        metadata=sample_collections[1]["metadata"],
    )
    assert result == sample_collections[1]

    # get_or_create = False creates new collection
    result, created = sysdb.create_collection(
        name=sample_collections[2]["name"],
        id=sample_collections[2]["id"],
        get_or_create=False,
        metadata=sample_collections[2]["metadata"],
    )
    assert result == sample_collections[2]

    # get_or_create = False fails if collection already exists
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            name=sample_collections[2]["name"],
            id=sample_collections[2]["id"],
            get_or_create=False,
            metadata=collection["metadata"],
        )

    # get_or_create = True overwrites metadata
    overlayed_metadata: Dict[str, Union[str, int, float]] = {
        "test_new_str": "new_str",
        "test_int": 1,
    }
    result, created = sysdb.create_collection(
        name=sample_collections[2]["name"],
        id=sample_collections[2]["id"],
        get_or_create=True,
        metadata=overlayed_metadata,
    )

    assert result["metadata"] == overlayed_metadata

    # get_or_create = False with None metadata does not overwrite metadata
    result, created = sysdb.create_collection(
        name=sample_collections[2]["name"],
        id=sample_collections[2]["id"],
        get_or_create=True,
        metadata=None,
    )
    assert result["metadata"] == overlayed_metadata


def test_create_get_delete_database_and_collection(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new database
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create a new collection in the new database
    sysdb.create_collection(
        id=sample_collections[0]["id"],
        name=sample_collections[0]["name"],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
    )

    # Create a new collection with the same id but different name in the new database
    # and expect an error
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[0]["id"],
            name="new_name",
            metadata=sample_collections[0]["metadata"],
            dimension=sample_collections[0]["dimension"],
            database="new_database",
            get_or_create=False,
        )

    # Create a new collection in the default database
    sysdb.create_collection(
        id=sample_collections[1]["id"],
        name=sample_collections[1]["name"],
        metadata=sample_collections[1]["metadata"],
        dimension=sample_collections[1]["dimension"],
    )

    # Check that the new database and collections exist
    result = sysdb.get_collections(
        name=sample_collections[0]["name"], database="new_database"
    )
    assert len(result) == 1
    sample_collections[0]["database"] = "new_database"
    assert result[0] == sample_collections[0]

    # Check that the collection in the default database exists
    result = sysdb.get_collections(name=sample_collections[1]["name"])
    assert len(result) == 1
    assert result[0] == sample_collections[1]

    # Get for a database that doesn't exist with a name that exists in the new database and expect no results
    assert (
        len(
            sysdb.get_collections(
                name=sample_collections[0]["name"], database="fake_db"
            )
        )
        == 0
    )

    # Delete the collection in the new database
    sysdb.delete_collection(id=sample_collections[0]["id"], database="new_database")

    # Check that the collection in the new database was deleted
    result = sysdb.get_collections(database="new_database")
    assert len(result) == 0

    # Check that the collection in the default database still exists
    result = sysdb.get_collections(name=sample_collections[1]["name"])
    assert len(result) == 1
    assert result[0] == sample_collections[1]

    # Delete the deleted collection in the default database and expect an error
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(id=sample_collections[0]["id"])

    # Delete the existing collection in the new database and expect an error
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(id=sample_collections[1]["id"], database="new_database")


def test_create_update_with_database(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new database
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create a new collection in the new database
    sysdb.create_collection(
        id=sample_collections[0]["id"],
        name=sample_collections[0]["name"],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
    )

    # Create a new collection in the default database
    sysdb.create_collection(
        id=sample_collections[1]["id"],
        name=sample_collections[1]["name"],
        metadata=sample_collections[1]["metadata"],
        dimension=sample_collections[1]["dimension"],
    )

    # Update the collection in the default database
    sysdb.update_collection(
        id=sample_collections[1]["id"],
        name="new_name_1",
    )

    # Check that the collection in the default database was updated
    result = sysdb.get_collections(id=sample_collections[1]["id"])
    assert len(result) == 1
    assert result[0]["name"] == "new_name_1"

    # Update the collection in the new database
    sysdb.update_collection(
        id=sample_collections[0]["id"],
        name="new_name_0",
    )

    # Check that the collection in the new database was updated
    result = sysdb.get_collections(
        id=sample_collections[0]["id"], database="new_database"
    )
    assert len(result) == 1
    assert result[0]["name"] == "new_name_0"

    # Try to create the collection in the default database in the new database and expect an error
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[1]["id"],
            name=sample_collections[1]["name"],
            metadata=sample_collections[1]["metadata"],
            dimension=sample_collections[1]["dimension"],
            database="new_database",
        )


def test_get_multiple_with_database(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new database
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create sample collections in the new database
    for collection in sample_collections:
        sysdb.create_collection(
            id=collection["id"],
            name=collection["name"],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
            database="new_database",
        )
        collection["database"] = "new_database"

    # Get all collections in the new database
    result = sysdb.get_collections(database="new_database")
    assert len(result) == len(sample_collections)
    assert sorted(result, key=lambda c: c["name"]) == sample_collections

    # Get all collections in the default database
    result = sysdb.get_collections()
    assert len(result) == 0


def test_create_database_with_tenants(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new tenant
    sysdb.create_tenant(name="tenant1")

    # Create tenant that already exits and expect an error
    with pytest.raises(UniqueConstraintError):
        sysdb.create_tenant(name="tenant1")

    with pytest.raises(UniqueConstraintError):
        sysdb.create_tenant(name=DEFAULT_TENANT)

    # Create a new database within this tenant and also in the default tenant
    sysdb.create_database(id=uuid.uuid4(), name="new_database", tenant="tenant1")
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create a new collection in the new tenant
    sysdb.create_collection(
        id=sample_collections[0]["id"],
        name=sample_collections[0]["name"],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
        tenant="tenant1",
    )
    sample_collections[0]["tenant"] = "tenant1"
    sample_collections[0]["database"] = "new_database"

    # Create a new collection in the default tenant
    sysdb.create_collection(
        id=sample_collections[1]["id"],
        name=sample_collections[1]["name"],
        metadata=sample_collections[1]["metadata"],
        dimension=sample_collections[1]["dimension"],
        database="new_database",
    )

    sample_collections[1]["database"] = "new_database"

    # Check that both tenants have the correct collections
    result = sysdb.get_collections(database="new_database", tenant="tenant1")
    assert len(result) == 1
    assert result[0] == sample_collections[0]

    result = sysdb.get_collections(database="new_database")
    assert len(result) == 1
    assert result[0] == sample_collections[1]

    # Creating a collection id that already exists in a tenant that does not have it
    # should error
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[0]["id"],
            name=sample_collections[0]["name"],
            metadata=sample_collections[0]["metadata"],
            dimension=sample_collections[0]["dimension"],
            database="new_database",
        )

    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[1]["id"],
            name=sample_collections[1]["name"],
            metadata=sample_collections[1]["metadata"],
            dimension=sample_collections[1]["dimension"],
            database="new_database",
            tenant="tenant1",
        )

    # A new tenant DOES NOT have a default database. This does not error, instead 0
    # results are returned
    result = sysdb.get_collections(database=DEFAULT_DATABASE, tenant="tenant1")
    assert len(result) == 0


def test_get_database_with_tenants(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new tenant
    sysdb.create_tenant(name="tenant1")

    # Get the tenant and check that it exists
    result = sysdb.get_tenant(name="tenant1")
    assert result["name"] == "tenant1"

    # Get a tenant that does not exist and expect an error
    with pytest.raises(NotFoundError):
        sysdb.get_tenant(name="tenant2")

    # Create a new database within this tenant
    sysdb.create_database(id=uuid.uuid4(), name="new_database", tenant="tenant1")

    # Get the database and check that it exists
    result = sysdb.get_database(name="new_database", tenant="tenant1")
    assert result["name"] == "new_database"
    assert result["tenant"] == "tenant1"

    # Get a database that does not exist in a tenant that does exist and expect an error
    with pytest.raises(NotFoundError):
        sysdb.get_database(name="new_database1", tenant="tenant1")

    # Get a database that does not exist in a tenant that does not exist and expect an
    # error
    with pytest.raises(NotFoundError):
        sysdb.get_database(name="new_database1", tenant="tenant2")


# endregion

# region Segment tests
sample_segments = [
    Segment(
        id=uuid.UUID("00000000-d7d7-413b-92e1-731098a6e492"),
        type="test_type_a",
        scope=SegmentScope.VECTOR,
        topic=None,
        collection=sample_collections[0]["id"],
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
    ),
    Segment(
        id=uuid.UUID("11111111-d7d7-413b-92e1-731098a6e492"),
        type="test_type_b",
        topic="test_topic_2",
        scope=SegmentScope.VECTOR,
        collection=sample_collections[1]["id"],
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
    ),
    Segment(
        id=uuid.UUID("22222222-d7d7-413b-92e1-731098a6e492"),
        type="test_type_b",
        topic="test_topic_3",
        scope=SegmentScope.METADATA,
        collection=None,
        metadata={"test_str": "str3", "test_int": 3, "test_float": 3.3},
    ),
]


def test_create_get_delete_segments(sysdb: SysDB) -> None:
    sysdb.reset_state()

    for collection in sample_collections:
        sysdb.create_collection(
            id=collection["id"],
            name=collection["name"],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
        )

    for segment in sample_segments:
        sysdb.create_segment(segment)

    results = sysdb.get_segments()
    results = sorted(results, key=lambda c: c["id"])

    assert results == sample_segments

    # Duplicate create fails
    with pytest.raises(UniqueConstraintError):
        sysdb.create_segment(sample_segments[0])

    # Find by id
    for segment in sample_segments:
        result = sysdb.get_segments(id=segment["id"])
        assert result == [segment]

    # Find by type
    result = sysdb.get_segments(type="test_type_a")
    assert result == sample_segments[:1]

    result = sysdb.get_segments(type="test_type_b")
    assert sorted(result, key=lambda c: c["type"]) == sample_segments[1:]

    # Find by collection ID
    result = sysdb.get_segments(collection=sample_collections[0]["id"])
    assert result == sample_segments[:1]

    # Find by type and collection ID (positive case)
    result = sysdb.get_segments(
        type="test_type_a", collection=sample_collections[0]["id"]
    )
    assert result == sample_segments[:1]

    # Find by type and collection ID (negative case)
    result = sysdb.get_segments(
        type="test_type_b", collection=sample_collections[0]["id"]
    )
    assert result == []

    # Delete
    s1 = sample_segments[0]
    sysdb.delete_segment(s1["id"])

    results = sysdb.get_segments()
    assert s1 not in results
    assert len(results) == len(sample_segments) - 1
    assert sorted(results, key=lambda c: c["type"]) == sample_segments[1:]

    # Duplicate delete throws an exception
    with pytest.raises(NotFoundError):
        sysdb.delete_segment(s1["id"])


def test_update_segment(sysdb: SysDB) -> None:
    metadata: Dict[str, Union[str, int, float]] = {
        "test_str": "str1",
        "test_int": 1,
        "test_float": 1.3,
    }
    segment = Segment(
        id=uuid.uuid4(),
        type="test_type_a",
        scope=SegmentScope.VECTOR,
        topic="test_topic_a",
        collection=sample_collections[0]["id"],
        metadata=metadata,
    )

    sysdb.reset_state()
    for c in sample_collections:
        sysdb.create_collection(
            id=c["id"], name=c["name"], metadata=c["metadata"], dimension=c["dimension"]
        )

    sysdb.create_segment(segment)

    # Update topic to new value
    segment["topic"] = "new_topic"
    sysdb.update_segment(segment["id"], topic=segment["topic"])
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Update topic to None
    segment["topic"] = None
    sysdb.update_segment(segment["id"], topic=segment["topic"])
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Update collection to new value
    segment["collection"] = sample_collections[1]["id"]
    sysdb.update_segment(segment["id"], collection=segment["collection"])
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Update collection to None
    segment["collection"] = None
    sysdb.update_segment(segment["id"], collection=segment["collection"])
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Add a new metadata key
    metadata["test_str2"] = "str2"
    sysdb.update_segment(segment["id"], metadata={"test_str2": "str2"})
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Update a metadata key
    metadata["test_str"] = "str3"
    sysdb.update_segment(segment["id"], metadata={"test_str": "str3"})
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Delete a metadata key
    del metadata["test_str"]
    sysdb.update_segment(segment["id"], metadata={"test_str": None})
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]

    # Delete all metadata keys
    segment["metadata"] = None
    sysdb.update_segment(segment["id"], metadata=None)
    result = sysdb.get_segments(id=segment["id"])
    assert result == [segment]


# endregion
