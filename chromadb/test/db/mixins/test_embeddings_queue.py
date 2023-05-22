import pytest
from itertools import count
from typing import (
    Generator,
    List,
    Callable,
    Optional,
    Dict,
    Union,
    Iterator,
    Sequence,
    cast,
)
from chromadb.db.mixins.embeddings_queue import EmbeddingsQueue
from chromadb.ingest import RejectedEmbeddingException
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.types import (
    InsertEmbeddingRecord,
    EmbeddingRecord,
    DeleteEmbeddingRecord,
    EmbeddingDeleteRecord,
    ScalarEncoding,
    InsertType,
)
from chromadb.config import System, Settings
from pytest import FixtureRequest, approx


def sqlite() -> Generator[EmbeddingsQueue, None, None]:
    """Fixture generator for sqlite DB"""
    yield SqliteDB(System(Settings(sqlite_database=":memory:", allow_reset=True)))


def db_fixtures() -> List[Callable[[], Generator[EmbeddingsQueue, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=db_fixtures())
def db(request: FixtureRequest) -> Generator[EmbeddingsQueue, None, None]:
    yield next(request.param())


@pytest.fixture(scope="module")
def sample_embeddings() -> Iterator[InsertEmbeddingRecord]:
    def create_record(i: int) -> InsertEmbeddingRecord:
        vector = [i + i * 0.1, i + 1 + i * 0.1]
        metadata: Optional[Dict[str, Union[str, int, float]]]
        if i % 2 == 0:
            metadata = None
        else:
            metadata = {"str_key": f"value_{i}", "int_key": i, "float_key": i + i * 0.1}

        record = InsertEmbeddingRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=metadata,
            insert_type=InsertType.ADD,
        )
        return record

    return (create_record(i) for i in count())


class CapturingConsumer:
    embeddings: List[Union[EmbeddingRecord, EmbeddingDeleteRecord]]

    def __init__(self) -> None:
        self.embeddings = []

    def __call__(
        self, embeddings: Sequence[Union[EmbeddingRecord, EmbeddingDeleteRecord]]
    ) -> None:
        self.embeddings.extend(embeddings)


def assert_approx_equal(a: Sequence[float], b: Sequence[float]) -> None:
    for i, j in zip(a, b):
        assert approx(i) == approx(j)


def assert_records_match(
    inserted_records: Sequence[Union[InsertEmbeddingRecord, DeleteEmbeddingRecord]],
    consumed_records: Sequence[Union[EmbeddingRecord, EmbeddingDeleteRecord]],
) -> None:
    """Given a list of inserted and consumed records, make sure they match"""
    assert len(consumed_records) == len(inserted_records)
    for inserted, consumed in zip(inserted_records, consumed_records):
        if "delete_id" in inserted:
            inserted = cast(DeleteEmbeddingRecord, inserted)
            consumed = cast(EmbeddingDeleteRecord, consumed)
            assert inserted["delete_id"] == consumed["delete_id"]
        else:
            inserted = cast(InsertEmbeddingRecord, inserted)
            consumed = cast(EmbeddingRecord, consumed)
            assert_approx_equal(consumed["embedding"], inserted["embedding"])
            assert consumed.get("encoding", None) == inserted.get("encoding", None)
            assert consumed.get("metadata", None) == inserted.get("metadata", None)


def test_backfill(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()

    embeddings = [next(sample_embeddings) for _ in range(3)]

    db.create_topic("test_topic")
    for e in embeddings:
        db.submit_embedding("test_topic", e)

    consume_fn = CapturingConsumer()
    db.subscribe("test_topic", consume_fn, start=db.min_seqid())

    assert_records_match(embeddings, consume_fn.embeddings)


def test_notifications(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic")

    embeddings: List[InsertEmbeddingRecord] = []

    consume_fn = CapturingConsumer()

    db.subscribe("test_topic", consume_fn, start=db.min_seqid())

    for i in range(10):
        e = next(sample_embeddings)
        embeddings.append(e)
        db.submit_embedding("test_topic", e)
        assert_records_match(embeddings, consume_fn.embeddings)


def test_sync_failure(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic")

    def failing_consumer(
        embeddings: Sequence[Union[EmbeddingRecord, EmbeddingDeleteRecord]]
    ) -> None:
        raise RejectedEmbeddingException("test failure")

    db.subscribe("test_topic", failing_consumer, start=db.min_seqid())

    e = next(sample_embeddings)
    with pytest.raises(Exception):
        db.submit_embedding("test_topic", e, sync=True)

    second_consumer = CapturingConsumer()
    db.subscribe("test_topic", second_consumer, start=db.min_seqid())
    assert second_consumer.embeddings == []


def test_async_failure(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic")

    def failing_consumer(
        embeddings: Sequence[Union[EmbeddingRecord, EmbeddingDeleteRecord]]
    ) -> None:
        raise RejectedEmbeddingException("test failure")

    db.subscribe("test_topic", failing_consumer, start=db.min_seqid())
    e = next(sample_embeddings)
    db.submit_embedding("test_topic", e, sync=False)

    second_consumer = CapturingConsumer()
    db.subscribe("test_topic", second_consumer, start=db.min_seqid())
    assert_records_match([e], second_consumer.embeddings)


def test_multiple_topics(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic_1")
    db.create_topic("test_topic_2")

    embeddings_1: List[InsertEmbeddingRecord] = []
    embeddings_2: List[InsertEmbeddingRecord] = []

    consume_fn_1 = CapturingConsumer()
    consume_fn_2 = CapturingConsumer()

    db.subscribe("test_topic_1", consume_fn_1, start=db.min_seqid())
    db.subscribe("test_topic_2", consume_fn_2, start=db.min_seqid())

    for i in range(10):
        e_1 = next(sample_embeddings)
        embeddings_1.append(e_1)
        db.submit_embedding("test_topic_1", e_1)
        assert_records_match(embeddings_1, consume_fn_1.embeddings)

        e_2 = next(sample_embeddings)
        embeddings_2.append(e_2)
        db.submit_embedding("test_topic_2", e_2)
        assert_records_match(embeddings_2, consume_fn_2.embeddings)


def test_start_seq_id(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic")

    consume_fn_1 = CapturingConsumer()
    consume_fn_2 = CapturingConsumer()

    db.subscribe("test_topic", consume_fn_1, start=db.min_seqid())

    embeddings = []
    for _ in range(5):
        e = next(sample_embeddings)
        embeddings.append(e)
        db.submit_embedding("test_topic", e)

    assert_records_match(embeddings, consume_fn_1.embeddings)

    start = consume_fn_1.embeddings[-1]["seq_id"]
    db.subscribe("test_topic", consume_fn_2, start=start)
    for _ in range(5):
        e = next(sample_embeddings)
        embeddings.append(e)
        db.submit_embedding("test_topic", e)

    assert_records_match(embeddings[-5:], consume_fn_2.embeddings)


def test_end_seq_id(
    db: EmbeddingsQueue, sample_embeddings: Iterator[InsertEmbeddingRecord]
) -> None:
    db.reset()
    db.create_topic("test_topic")

    consume_fn_1 = CapturingConsumer()
    consume_fn_2 = CapturingConsumer()

    db.subscribe("test_topic", consume_fn_1, start=db.min_seqid())

    embeddings = []
    for _ in range(10):
        e = next(sample_embeddings)
        embeddings.append(e)
        db.submit_embedding("test_topic", e)

    assert_records_match(embeddings, consume_fn_1.embeddings)

    end = consume_fn_1.embeddings[-5]["seq_id"]
    db.subscribe("test_topic", consume_fn_2, start=db.min_seqid(), end=end)

    assert_records_match(embeddings[:6], consume_fn_2.embeddings)
