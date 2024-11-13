import os
import functools
import shutil
import tempfile
import pytest
from typing import Generator, List, Callable, Dict, Union

from chromadb.db.impl.grpc.client import GrpcSysDB
from chromadb.db.impl.grpc.server import GrpcMockSysDB
from chromadb.errors import NotFoundError, UniqueConstraintError
from chromadb.test.conftest import find_free_port
from chromadb.types import Collection, Segment, SegmentScope
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import (
    DEFAULT_DATABASE,
    DEFAULT_TENANT,
    System,
    Settings,
)
from chromadb.db.system import SysDB
from pytest import FixtureRequest
import uuid
from chromadb.api.configuration import CollectionConfigurationInternal
import logging

logger = logging.getLogger(__name__)

TENANT = "default"
NAMESPACE = "default"

# These are the sample collections that are used in the tests below. Tests can override
# the fields as needed.
sample_collections: List[Collection] = [
    Collection(
        id=uuid.UUID(int=1),
        name="test_collection_1",
        configuration=CollectionConfigurationInternal(),
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
        dimension=128,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
        version=0,
    ),
    Collection(
        id=uuid.UUID(int=2),
        name="test_collection_2",
        configuration=CollectionConfigurationInternal(),
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
        dimension=None,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
        version=0,
    ),
    Collection(
        id=uuid.UUID(int=3),
        name="test_collection_3",
        configuration=CollectionConfigurationInternal(),
        metadata={"test_str": "str3", "test_int": 3, "test_float": 3.3},
        dimension=None,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
        version=0,
    ),
    Collection(
        id=uuid.UUID(int=4),
        name="test_collection_4",
        configuration=CollectionConfigurationInternal(),
        metadata={"test_str": "str4", "test_int": 4, "test_float": 4.4},
        dimension=None,
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
        version=0,
    ),
]


def sqlite() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    db = SqliteDB(
        System(
            Settings(
                allow_reset=True,
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
    port = find_free_port()

    system = System(
        Settings(
            allow_reset=True,
            chroma_server_grpc_port=port,
        )
    )
    system.instance(GrpcMockSysDB)
    client = system.instance(GrpcSysDB)
    system.start()
    client.reset_and_wait_for_ready()
    yield client
    system.stop()


def grpc_with_real_server() -> Generator[SysDB, None, None]:
    logger.debug("Setting up grpc_with_real_server")
    system = System(
        Settings(
            allow_reset=True,
            chroma_server_grpc_port=50051,
        )
    )
    client = system.instance(GrpcSysDB)
    logger.debug("Starting system")
    system.start()
    logger.debug("Resetting client and waiting for ready")
    client.reset_and_wait_for_ready()
    logger.debug("grpc_with_real_server setup complete")
    yield client
    logger.debug("Stopping system in grpc_with_real_server")
    system.stop()


def db_fixtures() -> List[Callable[[], Generator[SysDB, None, None]]]:
    if "CHROMA_CLUSTER_TEST_ONLY" in os.environ:
        return [grpc_with_real_server]
    else:
        return [sqlite, sqlite_persistent, grpc_with_mock_server]


@pytest.fixture(scope="module", params=db_fixtures())
def sysdb(request: FixtureRequest) -> Generator[SysDB, None, None]:
    logger.debug(f"Setting up sysdb fixture with {request.param.__name__}")
    yield next(request.param())
    logger.debug("Tearing down sysdb fixture")

def sample_segment(collection_id: uuid.UUID = uuid.uuid4(),
                   segment_type: str = "test_type_a",
                   scope: SegmentScope = SegmentScope.VECTOR,
                   metadata: Dict[str, Union[str, int, float]] = {
                       "test_str": "str1",
                       "test_int": 1,
                       "test_float": 1.3,
                   },
) -> Segment:
    return Segment(
        id=uuid.uuid4(),
        type=segment_type,
        scope=scope,
        collection=collection_id,
        metadata=metadata,
    )

# region Collection tests
def test_create_get_delete_collections(sysdb: SysDB) -> None:
    logger.debug("Resetting state")
    sysdb.reset_state()

    for collection in sample_collections:
        logger.debug(f"Creating collection: {collection.name}")
        sysdb.create_collection(
            id=collection.id,
            name=collection.name,
            configuration=collection.get_configuration(),
            segments=[
                Segment(
                    id=uuid.uuid4(),
                    type="test_type_a",
                    scope=SegmentScope.VECTOR,
                    collection=collection.id,
                    metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
                )
            ],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
        )
        collection["database"] = DEFAULT_DATABASE
        collection["tenant"] = DEFAULT_TENANT

    logger.debug("Getting all collections")
    results = sysdb.get_collections()
    results = sorted(results, key=lambda c: c.name)

    assert sorted(results, key=lambda c: c.name) == sample_collections

    # Duplicate create fails
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            name=sample_collections[0].name,
            id=sample_collections[0].id,
            configuration=sample_collections[0].get_configuration(),
            segments=[
                Segment(
                    id=uuid.uuid4(),
                    type="test_type_a",
                    scope=SegmentScope.VECTOR,
                    collection=sample_collections[0].id,
                    metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
                )
            ],
        )

    # Find by name
    for collection in sample_collections:
        result = sysdb.get_collections(name=collection["name"])
        assert result == [collection]

    # Find by id
    for collection in sample_collections:
        result = sysdb.get_collections(id=collection["id"])
        assert result == [collection]

    # Delete
    c1 = sample_collections[0]
    sysdb.delete_collection(c1.id)

    results = sysdb.get_collections()
    assert c1 not in results
    assert len(results) == len(sample_collections) - 1
    assert sorted(results, key=lambda c: c.name) == sample_collections[1:]

    by_id_result = sysdb.get_collections(id=c1["id"])
    assert by_id_result == []

    # Duplicate delete throws an exception
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(c1.id)


def test_update_collections(sysdb: SysDB) -> None:
    coll = Collection(
        name=sample_collections[0].name,
        id=sample_collections[0].id,
        configuration=sample_collections[0].get_configuration(),
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database=DEFAULT_DATABASE,
        tenant=DEFAULT_TENANT,
        version=0,
    )

    sysdb.reset_state()

    sysdb.create_collection(
        id=coll.id,
        name=coll.name,
        configuration=coll.get_configuration(),
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=coll.id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ],
        metadata=coll["metadata"],
        dimension=coll["dimension"],
    )

    # Update name
    coll["name"] = "new_name"
    sysdb.update_collection(coll.id, name=coll.name)
    result = sysdb.get_collections(name=coll.name)
    assert result == [coll]

    # Update dimension
    coll["dimension"] = 128
    sysdb.update_collection(coll.id, dimension=coll.dimension)
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]

    # Reset the metadata
    coll["metadata"] = {"test_str2": "str2"}
    sysdb.update_collection(coll.id, metadata=coll["metadata"])
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]

    # Delete all metadata keys
    coll["metadata"] = None
    sysdb.update_collection(coll.id, metadata=None)
    result = sysdb.get_collections(id=coll["id"])
    assert result == [coll]


def test_get_or_create_collection(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # get_or_create = True returns existing collection
    collection = sample_collections[0]

    sysdb.create_collection(
        id=collection.id,
        name=collection.name,
        configuration=collection.get_configuration(),
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=collection.id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ],
        metadata=collection["metadata"],
        dimension=collection["dimension"],
    )

    # Create collection with same name, but different id.
    # Since get_or_create is true, it should return the existing collection.
    result, created = sysdb.create_collection(
        name=collection.name,
        id=uuid.uuid4(),
        configuration=collection.get_configuration(),
        get_or_create=True,
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=sample_collections[1].id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ], # This could have been empty - [].
        metadata=collection["metadata"],
    )
    assert result == collection

    # Only one collection with the same name exists
    get_result = sysdb.get_collections(name=collection["name"])
    assert get_result == [collection]

    # get_or_create = True creates new collection
    result, created = sysdb.create_collection(
        name=sample_collections[1].name,
        id=sample_collections[1].id,
        configuration=sample_collections[1].get_configuration(),
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=sample_collections[1].id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ],
        get_or_create=True,
        metadata=sample_collections[1]["metadata"],
    )
    assert result == sample_collections[1]

    # get_or_create = False creates new collection
    result, created = sysdb.create_collection(
        name=sample_collections[2].name,
        id=sample_collections[2].id,
        configuration=sample_collections[2].get_configuration(),
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=sample_collections[2].id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ],
        get_or_create=False,
        metadata=sample_collections[2]["metadata"],
    )
    assert result == sample_collections[2]

    # get_or_create = False fails if collection already exists
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            name=sample_collections[2].name,
            id=sample_collections[2].id,
            configuration=sample_collections[2].get_configuration(),
            get_or_create=False,
            segments=[
                Segment(
                    id=uuid.uuid4(),
                    type="test_type_a",
                    scope=SegmentScope.VECTOR,
                    collection=sample_collections[2].id,
                    metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
                )
            ],
            metadata=collection["metadata"],
        )

    # get_or_create = True does not overwrite metadata
    overlayed_metadata: Dict[str, Union[str, int, float]] = {
        "test_new_str": "new_str",
        "test_int": 1,
    }

    result, created = sysdb.create_collection(
        name=sample_collections[2].name,
        id=sample_collections[2].id,
        configuration=sample_collections[2].get_configuration(),
        segments=[
            Segment(
                id=uuid.uuid4(),
                type="test_type_a",
                scope=SegmentScope.VECTOR,
                collection=sample_collections[2].id,
                metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
            )
        ],
        get_or_create=True,
        metadata=overlayed_metadata,
    )

    assert result["metadata"] != overlayed_metadata
    assert result["metadata"] == sample_collections[2]["metadata"]

    # get_or_create = True with None metadata does not overwrite metadata
    result, created = sysdb.create_collection(
        name=sample_collections[2].name,
        id=sample_collections[2].id,
        configuration=sample_collections[2].get_configuration(),
        segments=[sample_segment(sample_collections[2].id)],
        get_or_create=True,
        metadata=None,
    )
    assert result["metadata"] == sample_collections[2]["metadata"]


def test_create_get_delete_database_and_collection(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new database
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create a new collection in the new database
    sysdb.create_collection(
        id=sample_collections[0].id,
        name=sample_collections[0].name,
        configuration=sample_collections[0].get_configuration(),
        segments=[sample_segment(sample_collections[0].id)],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
    )

    # Create a new collection with the same id but different name in the new database
    # and expect an error
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[0].id,
            name="new_name",
            configuration=sample_collections[0].get_configuration(),
            metadata=sample_collections[0]["metadata"],
            dimension=sample_collections[0]["dimension"],
            segments=[sample_segment(sample_collections[0].id)],
            database="new_database",
            get_or_create=False,
        )

    # Create a new collection in the default database
    sysdb.create_collection(
        id=sample_collections[1].id,
        name=sample_collections[1].name,
        configuration=sample_collections[1].get_configuration(),
        metadata=sample_collections[1]["metadata"],
        dimension=sample_collections[1]["dimension"],
        segments=[sample_segment(sample_collections[1].id)],
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
    sysdb.delete_collection(id=sample_collections[0].id, database="new_database")

    # Check that the collection in the new database was deleted
    result = sysdb.get_collections(database="new_database")
    assert len(result) == 0

    # Check that the collection in the default database still exists
    result = sysdb.get_collections(name=sample_collections[1].name)
    assert len(result) == 1
    assert result[0] == sample_collections[1]

    # Delete the deleted collection in the default database and expect an error
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(id=sample_collections[0].id)

    # Delete the existing collection in the new database and expect an error
    with pytest.raises(NotFoundError):
        sysdb.delete_collection(id=sample_collections[1].id, database="new_database")


def test_create_update_with_database(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Create a new database
    sysdb.create_database(id=uuid.uuid4(), name="new_database")

    # Create a new collection in the new database
    sysdb.create_collection(
        id=sample_collections[0].id,
        name=sample_collections[0].name,
        configuration=sample_collections[0].get_configuration(),
        segments=[sample_segment(sample_collections[0].id)],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
    )

    # Create a new collection in the default database
    sysdb.create_collection(
        id=sample_collections[1].id,
        name=sample_collections[1].name,
        configuration=sample_collections[1].get_configuration(),
        segments=[sample_segment(sample_collections[1].id)],
        metadata=sample_collections[1]["metadata"],
        dimension=sample_collections[1]["dimension"],
    )

    # Update the collection in the default database
    sysdb.update_collection(
        id=sample_collections[1].id,
        name="new_name_1",
    )

    # Check that the collection in the default database was updated
    result = sysdb.get_collections(id=sample_collections[1]["id"])
    assert len(result) == 1
    assert result[0]["name"] == "new_name_1"

    # Update the collection in the new database
    sysdb.update_collection(
        id=sample_collections[0].id,
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
            id=sample_collections[1].id,
            name=sample_collections[1].name,
            configuration=sample_collections[1].get_configuration(),
            segments=[sample_segment(sample_collections[1].id)],
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
            id=collection.id,
            name=collection.name,
            configuration=collection.get_configuration(),
            segments=[sample_segment(collection.id)],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
            database="new_database",
        )
        collection["database"] = "new_database"

    # Get all collections in the new database
    result = sysdb.get_collections(database="new_database")
    assert len(result) == len(sample_collections)
    assert sorted(result, key=lambda c: c.name) == sample_collections

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
        id=sample_collections[0].id,
        name=sample_collections[0].name,
        configuration=sample_collections[0].get_configuration(),
        segments=[sample_segment(sample_collections[0].id)],
        metadata=sample_collections[0]["metadata"],
        dimension=sample_collections[0]["dimension"],
        database="new_database",
        tenant="tenant1",
    )
    sample_collections[0]["tenant"] = "tenant1"
    sample_collections[0]["database"] = "new_database"

    # Create a new collection in the default tenant
    sysdb.create_collection(
        id=sample_collections[1].id,
        name=sample_collections[1].name,
        configuration=sample_collections[1].get_configuration(),
        segments=[sample_segment(sample_collections[1].id)],
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
            id=sample_collections[0].id,
            name=sample_collections[0].name,
            configuration=sample_collections[0].get_configuration(),
            segments=[sample_segment(sample_collections[0].id)],
            metadata=sample_collections[0]["metadata"],
            dimension=sample_collections[0]["dimension"],
            database="new_database",
        )

    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(
            id=sample_collections[1].id,
            name=sample_collections[1].name,
            configuration=sample_collections[1].get_configuration(),
            segments=[sample_segment(sample_collections[1].id)],
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
        collection=sample_collections[0].id,
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
    ),
    Segment(
        id=uuid.UUID("11111111-d7d7-413b-92e1-731098a6e492"),
        type="test_type_b",
        scope=SegmentScope.VECTOR,
        collection=sample_collections[1].id,
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
    ),
]


def test_create_get_delete_segments(sysdb: SysDB) -> None:
    sysdb.reset_state()

    # Keep track of segments created with a collection.
    segments_created_with_collection = []
    # Used to toggle between test_type_a and test_type_b
    toggle_type = False

    # Create collections along with segments.
    for collection in sample_collections:
        toggle_type = not toggle_type
        segment = sample_segment(
            collection_id=collection.id,
            segment_type="test_type_a" if toggle_type else "test_type_b",
        )
        segments_created_with_collection.append(segment)
        collection_result, created = sysdb.create_collection(
            id=collection.id,
            name=collection.name,
            configuration=collection.get_configuration(),
            segments=[segment],
            metadata=collection["metadata"],
            dimension=collection["dimension"],
        )
        assert created is True

    results: List[Segment] = []
    for collection in sample_collections:
        results.extend(sysdb.get_segments(collection=collection.id))
    results = sorted(results, key=lambda c: c["id"])
    sorted_segments = sorted(segments_created_with_collection, key=lambda c: c["id"])
    assert results == sorted_segments

    # Duplicate create fails
    with pytest.raises(UniqueConstraintError):
        sysdb.create_segment(segments_created_with_collection[0])

    # Find by id
    for segment in segments_created_with_collection:
        result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
        assert result == [segment]

    # Find by type
    result = sysdb.get_segments(type="test_type_a", collection=sample_collections[0].id)
    assert len(result) == 1
    assert result[0]["collection"] == sample_collections[0].id
    assert result[0] == segments_created_with_collection[0]

    result = sysdb.get_segments(type="test_type_b", collection=sample_collections[1].id)
    assert len(result) == 1
    assert result[0] == segments_created_with_collection[1]

    # Find by collection ID
    result = sysdb.get_segments(collection=sample_collections[0].id)
    assert len(result) == 1
    assert result[0] == segments_created_with_collection[0]

    # Find by type and collection ID (positive case)
    result = sysdb.get_segments(type="test_type_a", collection=sample_collections[0].id)
    assert len(result) == 1
    assert result[0] == segments_created_with_collection[0]

    # Find by type and collection ID (negative case)
    result = sysdb.get_segments(type="test_type_b", collection=sample_collections[0].id)
    assert len(result) == 0

    # Delete
    s1 = segments_created_with_collection[0]
    sysdb.delete_segment(s1["collection"], s1["id"])

    results = []
    for collection in sample_collections:
        results.extend(sysdb.get_segments(collection=collection.id))
    assert s1 not in results
    assert len(results) == len(segments_created_with_collection) - 1
    assert sorted(results, key=lambda c: c["id"]) == sorted(
        segments_created_with_collection[1:], key=lambda c: c["id"]
    )

    # Duplicate delete throws an exception
    with pytest.raises(NotFoundError):
        sysdb.delete_segment(s1["collection"], s1["id"])


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
        collection=sample_collections[0].id,
        metadata=metadata,
    )

    sysdb.reset_state()
    for c in sample_collections:
        sysdb.create_collection(
            id=c.id,
            name=c.name,
            configuration=c.get_configuration(),
            segments=[sample_segment(c.id)],
            metadata=c["metadata"],
            dimension=c["dimension"],
        )

    sysdb.create_segment(segment)

    # TODO: revisit update segment - push collection id

    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]

    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]

    # Add a new metadata key
    metadata["test_str2"] = "str2"
    sysdb.update_segment(
        segment["collection"], segment["id"], metadata={"test_str2": "str2"}
    )
    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]

    # Update a metadata key
    metadata["test_str"] = "str3"
    sysdb.update_segment(
        segment["collection"], segment["id"], metadata={"test_str": "str3"}
    )
    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]

    # Delete a metadata key
    del metadata["test_str"]
    sysdb.update_segment(
        segment["collection"], segment["id"], metadata={"test_str": None}
    )
    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]

    # Delete all metadata keys
    segment["metadata"] = None
    sysdb.update_segment(segment["collection"], segment["id"], metadata=None)
    result = sysdb.get_segments(id=segment["id"], collection=segment["collection"])
    result[0]["collection"] = segment["collection"]
    assert result == [segment]


# endregion
