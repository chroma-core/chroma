import asyncio
import os
import shutil
import tempfile
from uuid import UUID
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
    Tuple,
)
from chromadb.ingest import Producer, Consumer
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.test.conftest import ProducerFn
from chromadb.types import (
    OperationRecord,
    Operation,
    LogRecord,
    ScalarEncoding,
)
from chromadb.config import System, Settings
from pytest import FixtureRequest, approx
from asyncio import Event, wait_for, TimeoutError


def sqlite() -> Generator[Tuple[Producer, Consumer], None, None]:
    """Fixture generator for sqlite Producer + Consumer"""
    system = System(Settings(allow_reset=True))
    db = system.require(SqliteDB)
    system.start()
    yield db, db
    system.stop()


def sqlite_persistent() -> Generator[Tuple[Producer, Consumer], None, None]:
    """Fixture generator for sqlite_persistent Producer + Consumer"""
    save_path = tempfile.mkdtemp()
    system = System(
        Settings(allow_reset=True, is_persistent=True, persist_directory=save_path)
    )
    db = system.require(SqliteDB)
    system.start()
    yield db, db
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def fixtures() -> List[Callable[[], Generator[Tuple[Producer, Consumer], None, None]]]:
    fixtures = [sqlite, sqlite_persistent]
    if "CHROMA_CLUSTER_TEST_ONLY" in os.environ:
        # TODO: We should add the new log service here
        fixtures = []

    return fixtures


@pytest.fixture(scope="module", params=fixtures())
def producer_consumer(
    request: FixtureRequest,
) -> Generator[Tuple[Producer, Consumer], None, None]:
    yield next(request.param())


@pytest.fixture(scope="module")
def sample_embeddings() -> Iterator[OperationRecord]:
    def create_record(i: int) -> OperationRecord:
        vector = [i + i * 0.1, i + 1 + i * 0.1]
        metadata: Optional[Dict[str, Union[str, int, float]]]
        if i % 2 == 0:
            metadata = None
        else:
            metadata = {"str_key": f"value_{i}", "int_key": i, "float_key": i + i * 0.1}

        record = OperationRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=metadata,
            operation=Operation.ADD,
        )
        return record

    return (create_record(i) for i in count())


class CapturingConsumeFn:
    embeddings: List[LogRecord]
    waiters: List[Tuple[int, Event]]

    def __init__(self) -> None:
        """A function that captures embeddings and allows you to wait for a certain
        number of embeddings to be available. It must be constructed in the thread with
        the main event loop
        """
        self.embeddings = []
        self.waiters = []
        self._loop = asyncio.get_event_loop()

    def __call__(self, embeddings: Sequence[LogRecord]) -> None:
        self.embeddings.extend(embeddings)
        for n, event in self.waiters:
            if len(self.embeddings) >= n:
                # event.set() is not thread safe, so we need to call it in the main event loop
                self._loop.call_soon_threadsafe(event.set)

    async def get(self, n: int, timeout_secs: int = 10) -> Sequence[LogRecord]:
        "Wait until at least N embeddings are available, then return all embeddings"
        if len(self.embeddings) >= n:
            return self.embeddings[:n]
        else:
            event = Event()
            self.waiters.append((n, event))
            # timeout so we don't hang forever on failure
            await wait_for(event.wait(), timeout_secs)
            return self.embeddings[:n]


def assert_approx_equal(a: Sequence[float], b: Sequence[float]) -> None:
    for i, j in zip(a, b):
        assert approx(i) == approx(j)


def assert_records_match(
    inserted_records: Sequence[OperationRecord],
    consumed_records: Sequence[LogRecord],
) -> None:
    """Given a list of inserted and consumed records, make sure they match"""
    assert len(consumed_records) == len(inserted_records)
    for inserted, consumed in zip(inserted_records, consumed_records):
        assert inserted["id"] == consumed["operation_record"]["id"]
        assert inserted["operation"] == consumed["operation_record"]["operation"]
        assert inserted["encoding"] == consumed["operation_record"]["encoding"]
        assert inserted["metadata"] == consumed["operation_record"]["metadata"]

        if inserted["embedding"] is not None:
            assert consumed["operation_record"]["embedding"] is not None
            assert_approx_equal(
                inserted["embedding"], consumed["operation_record"]["embedding"]
            )


@pytest.mark.asyncio
async def test_backfill(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection_id = UUID("00000000-0000-0000-0000-000000000000")
    embeddings = produce_fns(producer, collection_id, sample_embeddings, 3)[0]

    consume_fn = CapturingConsumeFn()
    consumer.subscribe(collection_id, consume_fn, start=consumer.min_seqid())

    recieved = await consume_fn.get(3)
    assert_records_match(embeddings, recieved)


@pytest.mark.asyncio
async def test_notifications(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection_id = UUID("00000000-0000-0000-0000-000000000000")

    embeddings: List[OperationRecord] = []

    consume_fn = CapturingConsumeFn()

    consumer.subscribe(collection_id, consume_fn, start=consumer.min_seqid())

    for i in range(10):
        e = next(sample_embeddings)
        embeddings.append(e)
        producer.submit_embedding(collection_id, e)
        received = await consume_fn.get(i + 1)
        assert_records_match(embeddings, received)


@pytest.mark.asyncio
async def test_multiple_collections(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection_1 = UUID("00000000-0000-0000-0000-000000000001")
    collection_2 = UUID("00000000-0000-0000-0000-000000000002")

    embeddings_1: List[OperationRecord] = []
    embeddings_2: List[OperationRecord] = []

    consume_fn_1 = CapturingConsumeFn()
    consume_fn_2 = CapturingConsumeFn()

    consumer.subscribe(collection_1, consume_fn_1, start=consumer.min_seqid())
    consumer.subscribe(collection_2, consume_fn_2, start=consumer.min_seqid())

    for i in range(10):
        e_1 = next(sample_embeddings)
        embeddings_1.append(e_1)
        producer.submit_embedding(collection_1, e_1)
        results_2 = await consume_fn_1.get(i + 1)
        assert_records_match(embeddings_1, results_2)

        e_2 = next(sample_embeddings)
        embeddings_2.append(e_2)
        producer.submit_embedding(collection_2, e_2)
        results_2 = await consume_fn_2.get(i + 1)
        assert_records_match(embeddings_2, results_2)


@pytest.mark.asyncio
async def test_start_seq_id(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection = UUID("00000000-0000-0000-0000-000000000000")

    consume_fn_1 = CapturingConsumeFn()
    consume_fn_2 = CapturingConsumeFn()

    consumer.subscribe(collection, consume_fn_1, start=consumer.min_seqid())

    embeddings = produce_fns(producer, collection, sample_embeddings, 5)[0]

    results_1 = await consume_fn_1.get(5)
    assert_records_match(embeddings, results_1)

    start = consume_fn_1.embeddings[-1]["log_offset"]
    consumer.subscribe(collection, consume_fn_2, start=start)
    second_embeddings = produce_fns(producer, collection, sample_embeddings, 5)[0]
    assert isinstance(embeddings, list)
    embeddings.extend(second_embeddings)
    results_2 = await consume_fn_2.get(5)
    assert_records_match(embeddings[-5:], results_2)


@pytest.mark.asyncio
async def test_end_seq_id(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection = UUID("00000000-0000-0000-0000-000000000000")

    consume_fn_1 = CapturingConsumeFn()
    consume_fn_2 = CapturingConsumeFn()

    consumer.subscribe(collection, consume_fn_1, start=consumer.min_seqid())

    embeddings = produce_fns(producer, collection, sample_embeddings, 10)[0]

    results_1 = await consume_fn_1.get(10)
    assert_records_match(embeddings, results_1)

    end = consume_fn_1.embeddings[-5]["log_offset"]
    consumer.subscribe(collection, consume_fn_2, start=consumer.min_seqid(), end=end)

    results_2 = await consume_fn_2.get(6)
    assert_records_match(embeddings[:6], results_2)

    # Should never produce a 7th
    with pytest.raises(TimeoutError):
        _ = await wait_for(consume_fn_2.get(7), timeout=1)


@pytest.mark.asyncio
async def test_submit_batch(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection = UUID("00000000-0000-0000-0000-000000000000")

    embeddings = [next(sample_embeddings) for _ in range(100)]

    producer.submit_embeddings(collection, embeddings=embeddings)

    consume_fn = CapturingConsumeFn()
    consumer.subscribe(collection, consume_fn, start=consumer.min_seqid())

    recieved = await consume_fn.get(100)
    assert_records_match(embeddings, recieved)


@pytest.mark.asyncio
async def test_multiple_collections_batch(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
    produce_fns: ProducerFn,
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()

    N_TOPICS = 2
    consume_fns = [CapturingConsumeFn() for _ in range(N_TOPICS)]
    for i in range(N_TOPICS):
        consumer.subscribe(
            UUID(f"00000000-0000-0000-0000-00000000000{i}"),
            consume_fns[i],
            start=consumer.min_seqid(),
        )

    embeddings_n: List[List[OperationRecord]] = [[] for _ in range(N_TOPICS)]

    PRODUCE_BATCH_SIZE = 10
    N_TO_PRODUCE = 100
    total_produced = 0
    for i in range(N_TO_PRODUCE // PRODUCE_BATCH_SIZE):
        for n in range(N_TOPICS):
            embeddings_n[n].extend(
                produce_fns(
                    producer,
                    UUID(f"00000000-0000-0000-0000-00000000000{n}"),
                    sample_embeddings,
                    PRODUCE_BATCH_SIZE,
                )[0]
            )
            recieved = await consume_fns[n].get(total_produced + PRODUCE_BATCH_SIZE)
            assert_records_match(embeddings_n[n], recieved)
        total_produced += PRODUCE_BATCH_SIZE


@pytest.mark.asyncio
async def test_max_batch_size(
    producer_consumer: Tuple[Producer, Consumer],
    sample_embeddings: Iterator[OperationRecord],
) -> None:
    producer, consumer = producer_consumer
    producer.reset_state()
    consumer.reset_state()
    collection = UUID("00000000-0000-0000-0000-000000000000")
    max_batch_size = producer.max_batch_size
    assert max_batch_size > 0

    # Make sure that we can produce a batch of size max_batch_size
    embeddings = [next(sample_embeddings) for _ in range(max_batch_size)]
    consume_fn = CapturingConsumeFn()
    consumer.subscribe(collection, consume_fn, start=consumer.min_seqid())
    producer.submit_embeddings(collection, embeddings=embeddings)
    received = await consume_fn.get(max_batch_size, timeout_secs=120)
    assert_records_match(embeddings, received)

    embeddings = [next(sample_embeddings) for _ in range(max_batch_size + 1)]
    # Make sure that we can't produce a batch of size > max_batch_size
    with pytest.raises(ValueError) as e:
        producer.submit_embeddings(collection, embeddings=embeddings)
    assert "Cannot submit more than" in str(e.value)
