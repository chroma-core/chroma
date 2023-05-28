import pytest
from typing import Generator, List, Callable, Iterator, Dict, Optional, Union
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


@pytest.fixture(scope="module")
def sample_embeddings() -> Iterator[SubmitEmbeddingRecord]:
    def create_record(i: int) -> SubmitEmbeddingRecord:
        vector = [i + i * 0.1, i + 1 + i * 0.1]
        metadata: Optional[Dict[str, Union[str, int, float]]]
        if i % 2 == 0:
            metadata = None
        else:
            metadata = {"str_key": f"value_{i}", "int_key": i, "float_key": i + i * 0.1}

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
