import os
import shutil
import tempfile
import pytest
from typing import (
    Generator,
    List,
    Callable,
    Iterator,
    Dict,
    Optional,
    Union,
    Sequence,
    cast,
)

from chromadb.api.types import validate_metadata
from chromadb.config import System, Settings
from chromadb.db.base import ParameterValue, get_sql
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.test.conftest import ProducerFn
from chromadb.types import (
    OperationRecord,
    MetadataEmbeddingRecord,
    Operation,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
)
from pypika import Table
from chromadb.ingest import Producer
from chromadb.segment import MetadataReader
import uuid
import time

from chromadb.segment.impl.metadata.sqlite import SqliteMetadataSegment

from pytest import FixtureRequest
from itertools import count


def sqlite() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    settings = Settings(allow_reset=True, is_persistent=False)
    system = System(settings)
    system.start()
    yield system
    system.stop()


def sqlite_persistent() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    save_path = tempfile.mkdtemp()
    settings = Settings(
        allow_reset=True, is_persistent=True, persist_directory=save_path
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    return [sqlite, sqlite_persistent]


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: FixtureRequest) -> Generator[System, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def sample_embeddings() -> Iterator[OperationRecord]:
    def create_record(i: int) -> OperationRecord:
        vector = [i + i * 0.1, i + 1 + i * 0.1]
        metadata: Optional[Dict[str, Union[str, int, float, bool]]]
        if i == 0:
            metadata = None
        else:
            metadata = {
                "str_key": f"value_{i}",
                "int_key": i,
                "float_key": i + i * 0.1,
                "bool_key": True,
            }
            if i % 3 == 0:
                metadata["div_by_three"] = "true"
            if i % 2 == 0:
                metadata["bool_key"] = False
            metadata["chroma:document"] = _build_document(i)

        record = OperationRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=metadata,
            operation=Operation.ADD,
        )
        return record

    return (create_record(i) for i in count())


_digit_map = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


def _build_document(i: int) -> str:
    digits = list(str(i))
    return " ".join(_digit_map[d] for d in digits)


segment_definition = Segment(
    id=uuid.uuid4(),
    type="test_type",
    scope=SegmentScope.METADATA,
    collection=uuid.UUID(int=0),
    metadata=None,
)

segment_definition2 = Segment(
    id=uuid.uuid4(),
    type="test_type",
    scope=SegmentScope.METADATA,
    collection=uuid.UUID(int=1),
    metadata=None,
)


def sync(segment: MetadataReader, seq_id: SeqId) -> None:
    # Try for up to 5 seconds, then throw a TimeoutError
    start = time.time()
    while time.time() - start < 5:
        if segment.max_seqid() >= seq_id:
            return
        time.sleep(0.25)
    raise TimeoutError(f"Timed out waiting for seq_id {seq_id}")


def test_insert_and_count(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()

    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    max_id = produce_fns(producer, collection_id, sample_embeddings, 3)[1][-1]

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    sync(segment, max_id)

    assert segment.count() == 3

    for i in range(3):
        max_id = producer.submit_embedding(collection_id, next(sample_embeddings))

    sync(segment, max_id)

    assert segment.count() == 6


def assert_equiv_records(
    expected: Sequence[OperationRecord], actual: Sequence[MetadataEmbeddingRecord]
) -> None:
    assert len(expected) == len(actual)
    sorted_expected = sorted(expected, key=lambda r: r["id"])
    sorted_actual = sorted(actual, key=lambda r: r["id"])
    for e, a in zip(sorted_expected, sorted_actual):
        assert e["id"] == a["id"]
        assert e["metadata"] == a["metadata"]


def test_get(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    embeddings, seq_ids = produce_fns(producer, collection_id, sample_embeddings, 10)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    sync(segment, seq_ids[-1])

    # get with bool key
    result = segment.get_metadata(where={"bool_key": True})
    assert len(result) == 5

    result = segment.get_metadata(where={"bool_key": False})
    assert len(result) == 4

    # Get all records
    results = segment.get_metadata()
    assert seq_ids == [r["seq_id"] for r in results]
    assert_equiv_records(embeddings, results)

    # get by ID
    result = segment.get_metadata(ids=[e["id"] for e in embeddings[0:5]])
    assert_equiv_records(embeddings[0:5], result)

    # Get with limit and offset
    # Cannot rely on order(yet), but can rely on retrieving exactly the
    # whole set eventually
    ret: List[MetadataEmbeddingRecord] = []
    ret.extend(segment.get_metadata(limit=3))
    assert len(ret) == 3
    ret.extend(segment.get_metadata(limit=3, offset=3))
    assert len(ret) == 6
    ret.extend(segment.get_metadata(limit=3, offset=6))
    assert len(ret) == 9
    ret.extend(segment.get_metadata(limit=3, offset=9))
    assert len(ret) == 10
    assert_equiv_records(embeddings, ret)

    # Get with simple where
    result = segment.get_metadata(where={"div_by_three": "true"})
    assert len(result) == 3

    # Get with gt/gte/lt/lte on int keys
    result = segment.get_metadata(where={"int_key": {"$gt": 5}})
    assert len(result) == 4
    result = segment.get_metadata(where={"int_key": {"$gte": 5}})
    assert len(result) == 5
    result = segment.get_metadata(where={"int_key": {"$lt": 5}})
    assert len(result) == 4
    result = segment.get_metadata(where={"int_key": {"$lte": 5}})
    assert len(result) == 5

    # Get with gt/lt on float keys with float values
    result = segment.get_metadata(where={"float_key": {"$gt": 5.01}})
    assert len(result) == 5
    result = segment.get_metadata(where={"float_key": {"$lt": 4.99}})
    assert len(result) == 4

    # Get with gt/lt on float keys with int values
    result = segment.get_metadata(where={"float_key": {"$gt": 5}})
    assert len(result) == 5
    result = segment.get_metadata(where={"float_key": {"$lt": 5}})
    assert len(result) == 4

    # Get with gt/lt on int keys with float values
    result = segment.get_metadata(where={"int_key": {"$gt": 5.01}})
    assert len(result) == 4
    result = segment.get_metadata(where={"int_key": {"$lt": 4.99}})
    assert len(result) == 4

    # Get with $ne
    # Returns metadata that has an int_key, but not equal to 5
    result = segment.get_metadata(where={"int_key": {"$ne": 5}})
    assert len(result) == 8

    # get with multiple heterogenous conditions
    result = segment.get_metadata(where={"div_by_three": "true", "int_key": {"$gt": 5}})
    assert len(result) == 2

    # get with OR conditions
    result = segment.get_metadata(where={"$or": [{"int_key": 1}, {"int_key": 2}]})
    assert len(result) == 2

    # get with AND conditions
    result = segment.get_metadata(
        where={"$and": [{"int_key": 3}, {"float_key": {"$gt": 5}}]}
    )
    assert len(result) == 0
    result = segment.get_metadata(
        where={"$and": [{"int_key": 3}, {"float_key": {"$lt": 5}}]}
    )
    assert len(result) == 1


def test_fulltext(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    max_id = produce_fns(producer, collection_id, sample_embeddings, 100)[1][-1]

    sync(segment, max_id)

    result = segment.get_metadata(where={"chroma:document": "four two"})
    result2 = segment.get_metadata(ids=["embedding_42"])
    assert result == result2

    # Test single result
    result = segment.get_metadata(where_document={"$contains": "four two"})
    assert len(result) == 1

    # Test not_contains
    result = segment.get_metadata(where_document={"$not_contains": "four two"})
    assert len(result) == len(
        [i for i in range(1, 100) if "four two" not in _build_document(i)]
    )

    # Test many results
    result = segment.get_metadata(where_document={"$contains": "zero"})
    assert len(result) == 9

    # Test not_contains
    result = segment.get_metadata(where_document={"$not_contains": "zero"})
    assert len(result) == len(
        [i for i in range(1, 100) if "zero" not in _build_document(i)]
    )

    # test $and
    result = segment.get_metadata(
        where_document={"$and": [{"$contains": "four"}, {"$contains": "two"}]}
    )
    assert len(result) == 2
    assert set([r["id"] for r in result]) == {"embedding_42", "embedding_24"}

    result = segment.get_metadata(
        where_document={"$and": [{"$not_contains": "four"}, {"$not_contains": "two"}]}
    )
    assert len(result) == len(
        [
            i
            for i in range(1, 100)
            if "four" not in _build_document(i) and "two" not in _build_document(i)
        ]
    )

    # test $or
    result = segment.get_metadata(
        where_document={"$or": [{"$contains": "zero"}, {"$contains": "one"}]}
    )
    ones = [i for i in range(1, 100) if "one" in _build_document(i)]
    zeros = [i for i in range(1, 100) if "zero" in _build_document(i)]
    expected = set([f"embedding_{i}" for i in set(ones + zeros)])
    assert set([r["id"] for r in result]) == expected

    result = segment.get_metadata(
        where_document={"$or": [{"$not_contains": "zero"}, {"$not_contains": "one"}]}
    )
    assert len(result) == len(
        [
            i
            for i in range(1, 100)
            if "zero" not in _build_document(i) or "one" not in _build_document(i)
        ]
    )

    # test combo with where clause (negative case)
    result = segment.get_metadata(
        where={"int_key": {"$eq": 42}}, where_document={"$contains": "zero"}
    )
    assert len(result) == 0

    # test combo with where clause (positive case)
    result = segment.get_metadata(
        where={"int_key": {"$eq": 42}}, where_document={"$contains": "four"}
    )
    assert len(result) == 1

    # test partial words
    result = segment.get_metadata(where_document={"$contains": "zer"})
    assert len(result) == 9


def test_delete(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(producer, collection_id, sample_embeddings, 10)
    max_id = seq_ids[-1]

    sync(segment, max_id)

    assert segment.count() == 10
    results = segment.get_metadata(ids=["embedding_0"])
    assert_equiv_records(embeddings[:1], results)

    # Delete by ID
    delete_embedding = OperationRecord(
        id="embedding_0",
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
    )
    max_id = produce_fns(
        producer, collection_id, (delete_embedding for _ in range(1)), 1
    )[1][-1]

    sync(segment, max_id)

    assert segment.count() == 9
    assert segment.get_metadata(ids=["embedding_0"]) == []

    # Delete is idempotent
    max_id = produce_fns(
        producer, collection_id, (delete_embedding for _ in range(1)), 1
    )[1][-1]

    sync(segment, max_id)
    assert segment.count() == 9
    assert segment.get_metadata(ids=["embedding_0"]) == []

    # re-add
    max_id = producer.submit_embedding(collection_id, embeddings[0])
    sync(segment, max_id)
    assert segment.count() == 10
    results = segment.get_metadata(ids=["embedding_0"])


def test_update(system: System, sample_embeddings: Iterator[OperationRecord]) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    _test_update(sample_embeddings, producer, segment, collection_id, Operation.UPDATE)

    # Update nonexisting ID
    update_record = OperationRecord(
        id="no_such_id",
        metadata={"foo": "bar"},
        embedding=None,
        encoding=None,
        operation=Operation.UPDATE,
    )
    max_id = producer.submit_embedding(collection_id, update_record)
    sync(segment, max_id)
    results = segment.get_metadata(ids=["no_such_id"])
    assert len(results) == 0
    assert segment.count() == 3


def test_upsert(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    _test_update(sample_embeddings, producer, segment, collection_id, Operation.UPSERT)

    # upsert previously nonexisting ID
    update_record = OperationRecord(
        id="no_such_id",
        metadata={"foo": "bar"},
        embedding=None,
        encoding=None,
        operation=Operation.UPSERT,
    )
    max_id = produce_fns(
        producer=producer,
        collection_id=collection_id,
        embeddings=(update_record for _ in range(1)),
        n=1,
    )[1][-1]
    sync(segment, max_id)
    results = segment.get_metadata(ids=["no_such_id"])
    assert results[0]["metadata"] == {"foo": "bar"}


def _test_update(
    sample_embeddings: Iterator[OperationRecord],
    producer: Producer,
    segment: MetadataReader,
    collection_id: uuid.UUID,
    op: Operation,
) -> None:
    """test code common between update and upsert paths"""

    embeddings = [next(sample_embeddings) for i in range(3)]

    max_id = 0
    for e in embeddings:
        max_id = producer.submit_embedding(collection_id, e)

    sync(segment, max_id)

    results = segment.get_metadata(ids=["embedding_0"])
    assert_equiv_records(embeddings[:1], results)

    # Update embedding with no metadata
    update_record = OperationRecord(
        id="embedding_0",
        metadata={"chroma:document": "foo bar"},
        embedding=None,
        encoding=None,
        operation=op,
    )
    max_id = producer.submit_embedding(collection_id, update_record)
    sync(segment, max_id)
    results = segment.get_metadata(ids=["embedding_0"])
    assert results[0]["metadata"] == {"chroma:document": "foo bar"}
    results = segment.get_metadata(where_document={"$contains": "foo"})
    assert results[0]["metadata"] == {"chroma:document": "foo bar"}

    # Update and overrwrite key
    update_record = OperationRecord(
        id="embedding_0",
        metadata={"chroma:document": "biz buz"},
        embedding=None,
        encoding=None,
        operation=op,
    )
    max_id = producer.submit_embedding(collection_id, update_record)
    sync(segment, max_id)
    results = segment.get_metadata(ids=["embedding_0"])
    assert results[0]["metadata"] == {"chroma:document": "biz buz"}
    results = segment.get_metadata(where_document={"$contains": "biz"})
    assert results[0]["metadata"] == {"chroma:document": "biz buz"}
    results = segment.get_metadata(where_document={"$contains": "foo"})
    assert len(results) == 0

    # Update and add key
    update_record = OperationRecord(
        id="embedding_0",
        metadata={"baz": 42},
        embedding=None,
        encoding=None,
        operation=op,
    )
    max_id = producer.submit_embedding(collection_id, update_record)
    sync(segment, max_id)
    results = segment.get_metadata(ids=["embedding_0"])
    assert results[0]["metadata"] == {"chroma:document": "biz buz", "baz": 42}

    # Update and delete key
    update_record = OperationRecord(
        id="embedding_0",
        metadata={"chroma:document": None},
        embedding=None,
        encoding=None,
        operation=op,
    )
    max_id = producer.submit_embedding(collection_id, update_record)
    sync(segment, max_id)
    results = segment.get_metadata(ids=["embedding_0"])
    assert results[0]["metadata"] == {"baz": 42}
    results = segment.get_metadata(where_document={"$contains": "biz"})
    assert len(results) == 0


def test_limit(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()

    collection_id = cast(uuid.UUID, segment_definition["collection"])
    max_id = produce_fns(producer, collection_id, sample_embeddings, 3)[1][-1]

    collection_id_2 = cast(uuid.UUID, segment_definition2["collection"])
    max_id2 = produce_fns(producer, collection_id_2, sample_embeddings, 3)[1][-1]

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    segment2 = SqliteMetadataSegment(system, segment_definition2)
    segment2.start()

    sync(segment, max_id)
    sync(segment2, max_id2)

    assert segment.count() == 3

    for i in range(3):
        max_id = producer.submit_embedding(collection_id, next(sample_embeddings))

    sync(segment, max_id)

    assert segment.count() == 6

    res = segment.get_metadata(limit=3)
    assert len(res) == 3

    # if limit is negative, throw error
    with pytest.raises(ValueError):
        segment.get_metadata(limit=-1)

    # if offset is more than number of results, return empty list
    res = segment.get_metadata(limit=3, offset=10)
    assert len(res) == 0


def test_delete_segment(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(producer, collection_id, sample_embeddings, 10)
    max_id = seq_ids[-1]

    sync(segment, max_id)

    assert segment.count() == 10
    results = segment.get_metadata(ids=["embedding_0"])
    assert_equiv_records(embeddings[:1], results)
    _id = segment._id
    segment.delete()
    _db = system.instance(SqliteDB)
    t = Table("embeddings")
    q = (
        _db.querybuilder()
        .from_(t)
        .select(t.id)
        .where(t.segment_id == ParameterValue(_db.uuid_to_db(_id)))
    )
    sql, params = get_sql(q)
    with _db.tx() as cur:
        res = cur.execute(sql, params)
        # assert that the segment is gone
        assert len(res.fetchall()) == 0

    fts_t = Table("embedding_fulltext_search")
    q_fts = (
        _db.querybuilder()
        .from_(fts_t)
        .select()
        .where(
            fts_t.rowid.isin(
                _db.querybuilder()
                .from_(t)
                .select(t.id)
                .where(t.segment_id == ParameterValue(_db.uuid_to_db(_id)))
            )
        )
    )
    sql, params = get_sql(q_fts)
    with _db.tx() as cur:
        res = cur.execute(sql, params)
        # assert that all FTS rows are gone
        assert len(res.fetchall()) == 0


def test_delete_single_fts_record(
    system: System,
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    collection_id = segment_definition["collection"]
    # We know that the collection_id exists so we can cast
    collection_id = cast(uuid.UUID, collection_id)

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(producer, collection_id, sample_embeddings, 10)
    max_id = seq_ids[-1]

    sync(segment, max_id)

    assert segment.count() == 10
    results = segment.get_metadata(ids=["embedding_0"])
    assert_equiv_records(embeddings[:1], results)
    _id = segment._id
    _db = system.instance(SqliteDB)
    # Delete by ID
    delete_embedding = OperationRecord(
        id="embedding_0",
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
    )
    max_id = produce_fns(
        producer, collection_id, (delete_embedding for _ in range(1)), 1
    )[1][-1]
    t = Table("embeddings")

    sync(segment, max_id)
    fts_t = Table("embedding_fulltext_search")
    q_fts = (
        _db.querybuilder()
        .from_(fts_t)
        .select()
        .where(
            fts_t.rowid.isin(
                _db.querybuilder()
                .from_(t)
                .select(t.id)
                .where(t.segment_id == ParameterValue(_db.uuid_to_db(_id)))
                .where(t.embedding_id == ParameterValue(delete_embedding["id"]))
            )
        )
    )
    sql, params = get_sql(q_fts)
    with _db.tx() as cur:
        res = cur.execute(sql, params)
        # assert that the ids that are deleted from the segment are also gone from the fts table
        assert len(res.fetchall()) == 0


def test_metadata_validation_forbidden_key() -> None:
    with pytest.raises(ValueError, match="chroma:document"):
        validate_metadata(
            {"chroma:document": "this is not the document you are looking for"}
        )
