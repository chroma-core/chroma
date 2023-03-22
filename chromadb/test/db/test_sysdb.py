from chromadb.db import Segment
import chromadb.db.duckdb2
from chromadb.config import Settings
import pytest
import uuid


@pytest.fixture
def duckdb_db():
    return chromadb.db.duckdb2.DuckDB2(Settings(duckdb_database=":memory:"))


test_dbs = [duckdb_db]

test_segments = [
    Segment(
        id=uuid.uuid4(),
        type="test",
        scope="metadata",
        embedding_function="ef1",
        metadata={"foo": "bar", "baz": "qux"},
    ),
    Segment(
        id=uuid.uuid4(),
        type="test",
        scope="vector",
        embedding_function="ef2",
        metadata={"foo": "bar", "biz": "buz"},
    ),
]


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_segment_read_write(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    assert len(db.get_segments()) == 0

    db.create_segment(test_segments[0])

    assert db.get_segments()[0] == test_segments[0]

    db.create_segment(test_segments[1])

    assert db.get_segments(id=test_segments[0]["id"])[0] == test_segments[0]
    assert db.get_segments(id=test_segments[1]["id"])[0] == test_segments[1]

    assert db.get_segments(embedding_function="ef1")[0] == test_segments[0]
    assert db.get_segments(embedding_function="ef2")[0] == test_segments[1]

    assert db.get_segments(metadata={"baz": "qux"})[0] == test_segments[0]
    assert db.get_segments(metadata={"biz": "buz"})[0] == test_segments[1]
    assert db.get_segments(metadata={"foo": "bar"}) == test_segments

    assert db.get_segments(embedding_function="ef1", metadata={"foo": "bar"})[0] == test_segments[0]

    # TODO test other fetch mechanisms....
