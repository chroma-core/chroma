import pytest
from typing import Generator, List, Callable
from chromadb.types import Collection, Segment, SegmentScope
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings
from chromadb.db.system import SysDB
from chromadb.db.base import NotFoundError, UniqueConstraintError
from pytest import FixtureRequest
import uuid


def sqlite() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    yield SqliteDB(System(Settings(sqlite_database=":memory:", allow_reset=True)))


def db_fixtures() -> List[Callable[[], Generator[SysDB, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=db_fixtures())
def sysdb(request: FixtureRequest) -> Generator[SysDB, None, None]:
    yield next(request.param())


sample_collections = [
    Collection(
        id=uuid.uuid4(),
        name="test_collection_1",
        topic="test_topic_1",
        metadata={"test_str": "str1", "test_int": 1, "test_float": 1.3},
    ),
    Collection(
        id=uuid.uuid4(),
        name="test_collection_2",
        topic="test_topic_2",
        metadata={"test_str": "str2", "test_int": 2, "test_float": 2.3},
    ),
    Collection(
        id=uuid.uuid4(),
        name="test_collection_3",
        topic="test_topic_3",
        metadata={"test_str": "str3", "test_int": 3, "test_float": 3.3},
    ),
]


def test_create_get_delete_collections(sysdb: SysDB) -> None:
    sysdb.reset()

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
    sysdb.reset()

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
