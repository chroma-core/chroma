import os
import shutil
import tempfile
import pytest
from typing import Generator, List, Callable, Dict, Union
from chromadb.types import Collection, Segment, SegmentScope
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings
from chromadb.db.system import SysDB
from chromadb.db.base import NotFoundError, UniqueConstraintError
from pytest import FixtureRequest
import uuid


def sqlite() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    db = SqliteDB(System(Settings(allow_reset=True)))
    db.start()
    yield db
    db.stop()


def sqlite_persistent() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    save_path = tempfile.mkdtemp()
    db = SqliteDB(
        System(
            Settings(allow_reset=True, is_persistent=True, persist_directory=save_path)
        )
    )
    db.start()
    yield db
    db.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def db_fixtures() -> List[Callable[[], Generator[SysDB, None, None]]]:
    return [sqlite, sqlite_persistent]


@pytest.fixture(scope="module", params=db_fixtures())
def sysdb(request: FixtureRequest) -> Generator[SysDB, None, None]:
    yield next(request.param())


sample_collections = [
    Collection(
        id=uuid.uuid4(),
        name="test_collection_1",
        topic="test_topic_1",
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
        dimension=128,
    ),
    Collection(
        id=uuid.uuid4(),
        name="test_collection_2",
        topic="test_topic_2",
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
        dimension=None,
    ),
    Collection(
        id=uuid.uuid4(),
        name="test_collection_3",
        topic="test_topic_3",
        metadata={"test_str": "str3", "test_int": 3, "test_float": 3.3},
        dimension=None,
    ),
]


def test_create_get_delete_collections(sysdb: SysDB) -> None:
    sysdb.reset_state()

    for collection in sample_collections:
        sysdb.create_collection(collection)

    results = sysdb.get_collections()
    results = sorted(results, key=lambda c: c["name"])

    assert sorted(results, key=lambda c: c["name"]) == sample_collections

    # Duplicate create fails
    with pytest.raises(UniqueConstraintError):
        sysdb.create_collection(sample_collections[0])

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
    metadata: Dict[str, Union[str, int, float]] = {
        "test_str": "str1",
        "test_int": 1,
        "test_float": 1.3,
    }
    coll = Collection(
        id=uuid.uuid4(),
        name="test_collection_1",
        topic="test_topic_1",
        metadata=metadata,
        dimension=None,
    )

    sysdb.reset_state()

    sysdb.create_collection(coll)

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
        sysdb.create_collection(collection)

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
    assert result == sample_segments[1:]

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
        sysdb.create_collection(c)

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
