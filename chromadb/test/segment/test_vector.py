import pytest
from typing import Generator, List, Callable, Iterator
from chromadb.config import System, Settings
from chromadb.types import (
    SubmitEmbeddingRecord,
    Operation,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
)
from chromadb.ingest import Producer
from chromadb.segment import VectorReader
import uuid
import time

from chromadb.segment.impl.vector.local_hnsw import LocalHnswSegment

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
        vector = [i + i * 0.5, i + i * 0.5]
        record = SubmitEmbeddingRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=None,
            operation=Operation.ADD,
        )
        return record

    return (create_record(i) for i in count())


segment_definition = Segment(
    id=uuid.uuid4(),
    type="test_type",
    scope=SegmentScope.VECTOR,
    topic="persistent://test/test/test_topic_1",
    collection=None,
    metadata=None,
)


def sync(segment: VectorReader, seq_id: SeqId) -> None:
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

    segment = LocalHnswSegment(system, segment_definition)
    segment.start()

    sync(segment, max_id)

    assert segment.count() == 3
    for i in range(3):
        max_id = producer.submit_embedding(topic, next(sample_embeddings))

    sync(segment, max_id)
    assert segment.count() == 6


def test_get_vectors(
    system: System, sample_embeddings: Iterator[SubmitEmbeddingRecord]
) -> None:
    system.reset()
    producer = system.instance(Producer)

    topic = str(segment_definition["topic"])

    segment = LocalHnswSegment(system, segment_definition)
    segment.start()

    embeddings = [next(sample_embeddings) for i in range(10)]

    seq_ids: List[SeqId] = []
    for e in embeddings:
        seq_ids.append(producer.submit_embedding(topic, e))

    sync(segment, seq_ids[-1])

    # Get all items
    vectors = segment.get_vectors()
    assert len(vectors) == len(embeddings)
    vectors = sorted(vectors, key=lambda v: v["id"])
    for actual, expected, seq_id in zip(vectors, embeddings, seq_ids):
        assert actual["id"] == expected["id"]
        assert actual["embedding"] == expected["embedding"]
        assert actual["seq_id"] == seq_id

    # Get selected IDs
    ids = [e["id"] for e in embeddings[5:]]
    vectors = segment.get_vectors(ids=ids)
    assert len(vectors) == 5
    vectors = sorted(vectors, key=lambda v: v["id"])
    for actual, expected, seq_id in zip(vectors, embeddings[5:], seq_ids[5:]):
        assert actual["id"] == expected["id"]
        assert actual["embedding"] == expected["embedding"]
        assert actual["seq_id"] == seq_id
