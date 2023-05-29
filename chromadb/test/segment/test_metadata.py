import pytest
from typing import Generator, List, Callable, Iterator, Dict, Optional, Union, Sequence
from chromadb.config import System, Settings
from chromadb.types import (
    SubmitEmbeddingRecord,
    MetadataEmbeddingRecord,
    Operation,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
)
from chromadb.ingest import Producer
from chromadb.segment import MetadataReader
import uuid
import time

from chromadb.segment.impl.sqlite_metadata import SqliteMetadataSegment

from pytest import FixtureRequest
from itertools import count


def sqlite() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    settings = Settings(sqlite_database=":memory:", allow_reset=True)
    system = System(settings)
    system.start()
    yield system
    system.stop()


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: FixtureRequest) -> Generator[System, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def sample_embeddings() -> Iterator[SubmitEmbeddingRecord]:
    def create_record(i: int) -> SubmitEmbeddingRecord:
        vector = [i + i * 0.1, i + 1 + i * 0.1]
        metadata: Optional[Dict[str, Union[str, int, float]]]
        if i == 0:
            metadata = None
        else:
            metadata = {"str_key": f"value_{i}", "int_key": i, "float_key": i + i * 0.1}
            if i % 3 == 0:
                metadata["div_by_three"] = "true"

        record = SubmitEmbeddingRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=metadata,
            operation=Operation.ADD,
        )
        return record

    return (create_record(i) for i in count())


segment_definition = Segment(
    id=uuid.uuid4(),
    type="test_type",
    scope=SegmentScope.METADATA,
    topic="persistent://test/test/test_topic_1",
    collection=None,
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
    system: System, sample_embeddings: Iterator[SubmitEmbeddingRecord]
) -> None:
    system.reset()
    producer = system.instance(Producer)

    topic = str(segment_definition["topic"])

    max_id = 0
    for i in range(3):
        max_id = producer.submit_embedding(topic, next(sample_embeddings))

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    sync(segment, max_id)

    assert segment.count_metadata() == 3

    for i in range(3):
        max_id = producer.submit_embedding(topic, next(sample_embeddings))

    sync(segment, max_id)

    assert segment.count_metadata() == 6


def assert_equiv_records(
    expected: Sequence[SubmitEmbeddingRecord], actual: Sequence[MetadataEmbeddingRecord]
) -> None:
    assert len(expected) == len(actual)
    sorted_expected = sorted(expected, key=lambda r: r["id"])
    sorted_actual = sorted(actual, key=lambda r: r["id"])
    for e, a in zip(sorted_expected, sorted_actual):
        assert e["id"] == a["id"]
        assert e["metadata"] == a["metadata"]


def test_get(
    system: System, sample_embeddings: Iterator[SubmitEmbeddingRecord]
) -> None:
    system.reset()

    producer = system.instance(Producer)
    topic = str(segment_definition["topic"])

    embeddings = [next(sample_embeddings) for i in range(10)]

    seq_ids = []
    for e in embeddings:
        seq_ids.append(producer.submit_embedding(topic, e))

    segment = SqliteMetadataSegment(system, segment_definition)
    segment.start()

    sync(segment, seq_ids[-1])

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

    # get with multiple heterogenous conditions
    result = segment.get_metadata(where={"div_by_three": "true", "int_key": {"$gt": 5}})
    assert len(result) == 2
