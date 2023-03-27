from chromadb.types import Segment, Topic, EmbeddingFunction, ScalarEncoding
import chromadb.db.impls.duckdb
from chromadb.config import Settings
import pytest
import uuid


@pytest.fixture
def duckdb_db():
    return chromadb.db.impls.duckdb.DuckDB(Settings(duckdb_database=":memory:"))


test_dbs = [duckdb_db]

test_embedding_functions = [
    EmbeddingFunction(name="ef1", dimension=128, scalar_encoding=ScalarEncoding.FLOAT32),
    EmbeddingFunction(name="ef2", dimension=256, scalar_encoding=ScalarEncoding.INT32),
]

test_segments = [
    Segment(
        id=uuid.uuid4(),
        type="test",
        scope="metadata",
        topic=None,
        metadata={"foo": "bar", "baz": "qux"},
    ),
    Segment(
        id=uuid.uuid4(),
        type="test",
        scope="vector",
        topic="persistent://tenant/namespace/topic1",
        metadata={"foo": "bar", "biz": "buz"},
    ),
    Segment(id=uuid.uuid4(), type="test", scope="vector", topic=None, metadata=None),
]

test_topics = [
    Topic(
        name="persistent://tenant/namespace/topic1",
        embedding_function=test_embedding_functions[0]["name"],
        metadata={"foo": "bar", "baz": "qux"},
    ),
    Topic(
        name="persistent://tenant/namespace/topic2",
        embedding_function=test_embedding_functions[1]["name"],
        metadata={"foo": "bar", "biz": "buz"},
    ),
    Topic(name="persistent://tenant/namespace/topic3", embedding_function=None, metadata=None),
]


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_segment_read_write(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    assert len(db.get_segments()) == 0

    for embedding_function in test_embedding_functions:
        db.create_embedding_function(embedding_function)

    for topic in test_topics:
        db.create_topic(topic)

    db.create_segment(test_segments[0])

    assert db.get_segments()[0] == test_segments[0]

    db.create_segment(test_segments[1])
    db.create_segment(test_segments[2])

    assert db.get_segments(id=test_segments[0]["id"])[0] == test_segments[0]
    assert db.get_segments(id=test_segments[1]["id"])[0] == test_segments[1]
    assert db.get_segments(id=test_segments[2]["id"])[0] == test_segments[2]

    assert db.get_segments(metadata={"baz": "qux"})[0] == test_segments[0]
    assert db.get_segments(metadata={"biz": "buz"})[0] == test_segments[1]
    assert db.get_segments(metadata={"foo": "bar"}) == test_segments[:2]

    assert db.get_segments(scope="metadata", metadata={"foo": "bar"})[0] == test_segments[0]

    assert db.get_segments(topic="persistent://tenant/namespace/topic1")[0] == test_segments[1]
    assert len(db.get_segments(topic="no-such-topic")) == 0


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_embedding_function_read_write(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    assert len(db.get_embedding_functions()) == 0

    db.create_embedding_function(test_embedding_functions[0])

    assert db.get_embedding_functions()[0] == test_embedding_functions[0]

    db.create_embedding_function(test_embedding_functions[1])

    assert db.get_embedding_functions(name="ef1")[0] == test_embedding_functions[0]
    assert db.get_embedding_functions(name="ef2")[0] == test_embedding_functions[1]


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_topic_read_write(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    for embedding_function in test_embedding_functions:
        db.create_embedding_function(embedding_function)

    assert len(db.get_topics()) == 0

    db.create_topic(test_topics[0])

    assert db.get_topics()[0] == test_topics[0]
    assert len(db.get_topics()) == 1

    db.create_topic(test_topics[1])
    db.create_topic(test_topics[2])

    assert db.get_topics(name="persistent://tenant/namespace/topic1")[0] == test_topics[0]
    assert db.get_topics(name="persistent://tenant/namespace/topic2")[0] == test_topics[1]

    assert db.get_topics(embedding_function="ef1")[0] == test_topics[0]
    assert db.get_topics(embedding_function="ef2")[0] == test_topics[1]
