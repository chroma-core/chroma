import pytest
from typing import Generator, List, Callable, Iterator, Type, cast
from chromadb.config import System, Settings
from chromadb.test.conftest import ProducerFn
from chromadb.types import (
    SubmitEmbeddingRecord,
    VectorQuery,
    Operation,
    ScalarEncoding,
    Segment,
    SegmentScope,
    SeqId,
    Vector,
)
from chromadb.ingest import Producer
from chromadb.segment import VectorReader
import uuid
import time

from chromadb.segment.impl.vector.local_hnsw import (
    LocalHnswSegment,
)

from chromadb.segment.impl.vector.local_persistent_hnsw import (
    PersistentLocalHnswSegment,
)

from chromadb.test.property.strategies import test_hnsw_config
from pytest import FixtureRequest
from itertools import count
import tempfile
import os
import shutil


def sqlite() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    save_path = tempfile.mkdtemp()
    settings = Settings(
        allow_reset=True,
        is_persistent=False,
        persist_directory=save_path,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


def sqlite_persistent() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    save_path = tempfile.mkdtemp()
    settings = Settings(
        allow_reset=True,
        is_persistent=True,
        persist_directory=save_path,
    )
    system = System(settings)
    system.start()
    yield system
    system.stop()
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


# We will excercise in memory, persistent sqlite with both ephemeral and persistent hnsw.
# We technically never expose persitent sqlite with memory hnsw to users, but it's a valid
# configuration, so we test it here.
def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    return [sqlite, sqlite_persistent]


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: FixtureRequest) -> Generator[System, None, None]:
    yield next(request.param())


@pytest.fixture(scope="function")
def sample_embeddings() -> Iterator[SubmitEmbeddingRecord]:
    """Generate a sequence of embeddings with the property that for each embedding
    (other than the first and last), it's nearest neighbor is the previous in the
    sequence, and it's second nearest neighbor is the subsequent"""

    def create_record(i: int) -> SubmitEmbeddingRecord:
        vector = [i**1.1, i**1.1]
        record = SubmitEmbeddingRecord(
            id=f"embedding_{i}",
            embedding=vector,
            encoding=ScalarEncoding.FLOAT32,
            metadata=None,
            operation=Operation.ADD,
            collection_id=uuid.UUID(int=0),
        )
        return record

    return (create_record(i) for i in count())


def vector_readers() -> List[Type[VectorReader]]:
    return [LocalHnswSegment, PersistentLocalHnswSegment]


@pytest.fixture(scope="module", params=vector_readers())
def vector_reader(request: FixtureRequest) -> Generator[Type[VectorReader], None, None]:
    yield request.param


def create_random_segment_definition() -> Segment:
    return Segment(
        id=uuid.uuid4(),
        type="test_type",
        scope=SegmentScope.VECTOR,
        topic="persistent://test/test/test_topic_1",
        collection=None,
        metadata=test_hnsw_config,
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
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)

    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    max_id = produce_fns(
        producer=producer, topic=topic, n=3, embeddings=sample_embeddings
    )[1][-1]

    segment = vector_reader(system, segment_definition)
    segment.start()

    sync(segment, max_id)

    assert segment.count() == 3

    max_id = produce_fns(
        producer=producer, topic=topic, n=3, embeddings=sample_embeddings
    )[1][-1]

    sync(segment, max_id)
    assert segment.count() == 6


def approx_equal(a: float, b: float, epsilon: float = 0.0001) -> bool:
    return abs(a - b) < epsilon


def approx_equal_vector(a: Vector, b: Vector, epsilon: float = 0.0001) -> bool:
    return all(approx_equal(x, y, epsilon) for x, y in zip(a, b))


def test_get_vectors(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(
        producer=producer, topic=topic, embeddings=sample_embeddings, n=10
    )

    sync(segment, seq_ids[-1])

    # Get all items
    vectors = segment.get_vectors()
    assert len(vectors) == len(embeddings)
    vectors = sorted(vectors, key=lambda v: v["id"])
    for actual, expected, seq_id in zip(vectors, embeddings, seq_ids):
        assert actual["id"] == expected["id"]
        assert approx_equal_vector(
            actual["embedding"], cast(Vector, expected["embedding"])
        )
        assert actual["seq_id"] == seq_id

    # Get selected IDs
    ids = [e["id"] for e in embeddings[5:]]
    vectors = segment.get_vectors(ids=ids)
    assert len(vectors) == 5
    vectors = sorted(vectors, key=lambda v: v["id"])
    for actual, expected, seq_id in zip(vectors, embeddings[5:], seq_ids[5:]):
        assert actual["id"] == expected["id"]
        assert approx_equal_vector(
            actual["embedding"], cast(Vector, expected["embedding"])
        )
        assert actual["seq_id"] == seq_id


def test_ann_query(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(
        producer=producer, topic=topic, embeddings=sample_embeddings, n=100
    )

    sync(segment, seq_ids[-1])

    # Each item is its own nearest neighbor (one at a time)
    for e in embeddings:
        vector = cast(Vector, e["embedding"])
        query = VectorQuery(
            vectors=[vector],
            k=1,
            allowed_ids=None,
            options=None,
            include_embeddings=True,
        )
        results = segment.query_vectors(query)
        assert len(results) == 1
        assert len(results[0]) == 1
        assert results[0][0]["id"] == e["id"]
        assert results[0][0]["embedding"] is not None
        assert approx_equal_vector(results[0][0]["embedding"], vector)

    # Each item is its own nearest neighbor (all at once)
    vectors = [cast(Vector, e["embedding"]) for e in embeddings]
    query = VectorQuery(
        vectors=vectors, k=1, allowed_ids=None, options=None, include_embeddings=False
    )
    results = segment.query_vectors(query)
    assert len(results) == len(embeddings)
    for r, e in zip(results, embeddings):
        assert len(r) == 1
        assert r[0]["id"] == e["id"]

    # Each item's 3 nearest neighbors are itself and the item before and after
    test_embeddings = embeddings[1:-1]
    vectors = [cast(Vector, e["embedding"]) for e in test_embeddings]
    query = VectorQuery(
        vectors=vectors, k=3, allowed_ids=None, options=None, include_embeddings=False
    )
    results = segment.query_vectors(query)
    assert len(results) == len(test_embeddings)

    for r, e, i in zip(results, test_embeddings, range(1, len(test_embeddings))):
        assert len(r) == 3
        assert r[0]["id"] == embeddings[i]["id"]
        assert r[1]["id"] == embeddings[i - 1]["id"]
        assert r[2]["id"] == embeddings[i + 1]["id"]


def test_delete(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(
        producer=producer, topic=topic, embeddings=sample_embeddings, n=5
    )

    sync(segment, seq_ids[-1])
    assert segment.count() == 5

    delete_record = SubmitEmbeddingRecord(
        id=embeddings[0]["id"],
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
        collection_id=uuid.UUID(int=0),
    )
    assert isinstance(seq_ids, List)
    seq_ids.append(
        produce_fns(
            producer=producer,
            topic=topic,
            n=1,
            embeddings=(delete_record for _ in range(1)),
        )[1][0]
    )

    sync(segment, seq_ids[-1])

    # Assert that the record is gone using `count`
    assert segment.count() == 4

    # Assert that the record is gone using `get`
    assert segment.get_vectors(ids=[embeddings[0]["id"]]) == []
    results = segment.get_vectors()
    assert len(results) == 4
    # get_vectors returns results in arbitrary order
    results = sorted(results, key=lambda v: v["id"])
    for actual, expected in zip(results, embeddings[1:]):
        assert actual["id"] == expected["id"]
        assert approx_equal_vector(
            actual["embedding"], cast(Vector, expected["embedding"])
        )

    # Assert that the record is gone from KNN search
    vector = cast(Vector, embeddings[0]["embedding"])
    query = VectorQuery(
        vectors=[vector], k=10, allowed_ids=None, options=None, include_embeddings=False
    )
    knn_results = segment.query_vectors(query)
    assert len(results) == 4
    assert set(r["id"] for r in knn_results[0]) == set(e["id"] for e in embeddings[1:])

    # Delete is idempotent
    seq_ids.append(
        produce_fns(
            producer=producer,
            topic=topic,
            n=1,
            embeddings=(delete_record for _ in range(1)),
        )[1][0]
    )

    sync(segment, seq_ids[-1])

    assert segment.count() == 4


def _test_update(
    producer: Producer,
    topic: str,
    segment: VectorReader,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    operation: Operation,
) -> None:
    """Tests the common code paths between update & upsert"""

    embeddings = [next(sample_embeddings) for i in range(3)]

    seq_ids: List[SeqId] = []
    for e in embeddings:
        seq_ids.append(producer.submit_embedding(topic, e))

    sync(segment, seq_ids[-1])
    assert segment.count() == 3

    seq_ids.append(
        producer.submit_embedding(
            topic,
            SubmitEmbeddingRecord(
                id=embeddings[0]["id"],
                embedding=[10.0, 10.0],
                encoding=ScalarEncoding.FLOAT32,
                metadata=None,
                operation=operation,
                collection_id=uuid.UUID(int=0),
            ),
        )
    )

    sync(segment, seq_ids[-1])

    # Test new data from get_vectors
    assert segment.count() == 3
    results = segment.get_vectors()
    assert len(results) == 3
    results = segment.get_vectors(ids=[embeddings[0]["id"]])
    assert results[0]["embedding"] == [10.0, 10.0]

    # Test querying at the old location
    vector = cast(Vector, embeddings[0]["embedding"])
    query = VectorQuery(
        vectors=[vector], k=3, allowed_ids=None, options=None, include_embeddings=False
    )
    knn_results = segment.query_vectors(query)[0]
    assert knn_results[0]["id"] == embeddings[1]["id"]
    assert knn_results[1]["id"] == embeddings[2]["id"]
    assert knn_results[2]["id"] == embeddings[0]["id"]

    # Test querying at the new location
    vector = [10.0, 10.0]
    query = VectorQuery(
        vectors=[vector], k=3, allowed_ids=None, options=None, include_embeddings=False
    )
    knn_results = segment.query_vectors(query)[0]
    assert knn_results[0]["id"] == embeddings[0]["id"]
    assert knn_results[1]["id"] == embeddings[2]["id"]
    assert knn_results[2]["id"] == embeddings[1]["id"]


def test_update(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    _test_update(producer, topic, segment, sample_embeddings, Operation.UPDATE)

    # test updating a nonexistent record
    update_record = SubmitEmbeddingRecord(
        id="no_such_record",
        embedding=[10.0, 10.0],
        encoding=ScalarEncoding.FLOAT32,
        metadata=None,
        operation=Operation.UPDATE,
        collection_id=uuid.UUID(int=0),
    )
    seq_id = produce_fns(
        producer=producer,
        topic=topic,
        n=1,
        embeddings=(update_record for _ in range(1)),
    )[1][0]

    sync(segment, seq_id)

    assert segment.count() == 3
    assert segment.get_vectors(ids=["no_such_record"]) == []


def test_upsert(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    _test_update(producer, topic, segment, sample_embeddings, Operation.UPSERT)

    # test updating a nonexistent record
    upsert_record = SubmitEmbeddingRecord(
        id="no_such_record",
        embedding=[42, 42],
        encoding=ScalarEncoding.FLOAT32,
        metadata=None,
        operation=Operation.UPSERT,
        collection_id=uuid.UUID(int=0),
    )
    seq_id = produce_fns(
        producer=producer,
        topic=topic,
        n=1,
        embeddings=(upsert_record for _ in range(1)),
    )[1][0]

    sync(segment, seq_id)

    assert segment.count() == 4
    result = segment.get_vectors(ids=["no_such_record"])
    assert len(result) == 1
    assert approx_equal_vector(result[0]["embedding"], [42, 42])


def test_delete_without_add(
    system: System,
    vector_reader: Type[VectorReader],
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    assert segment.count() == 0

    delete_record = SubmitEmbeddingRecord(
        id="not_in_db",
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
        collection_id=uuid.UUID(int=0),
    )

    try:
        producer.submit_embedding(topic, delete_record)
    except BaseException:
        pytest.fail("Unexpected error. Deleting on an empty segment should not raise.")


def test_delete_with_local_segment_storage(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(
        producer=producer, topic=topic, embeddings=sample_embeddings, n=5
    )

    sync(segment, seq_ids[-1])
    assert segment.count() == 5

    delete_record = SubmitEmbeddingRecord(
        id=embeddings[0]["id"],
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
        collection_id=uuid.UUID(int=0),
    )
    assert isinstance(seq_ids, List)
    seq_ids.append(
        produce_fns(
            producer=producer,
            topic=topic,
            n=1,
            embeddings=(delete_record for _ in range(1)),
        )[1][0]
    )

    sync(segment, seq_ids[-1])

    # Assert that the record is gone using `count`
    assert segment.count() == 4

    # Assert that the record is gone using `get`
    assert segment.get_vectors(ids=[embeddings[0]["id"]]) == []
    results = segment.get_vectors()
    assert len(results) == 4
    # get_vectors returns results in arbitrary order
    results = sorted(results, key=lambda v: v["id"])
    for actual, expected in zip(results, embeddings[1:]):
        assert actual["id"] == expected["id"]
        assert approx_equal_vector(
            actual["embedding"], cast(Vector, expected["embedding"])
        )

    # Assert that the record is gone from KNN search
    vector = cast(Vector, embeddings[0]["embedding"])
    query = VectorQuery(
        vectors=[vector], k=10, allowed_ids=None, options=None, include_embeddings=False
    )
    knn_results = segment.query_vectors(query)
    assert len(results) == 4
    assert set(r["id"] for r in knn_results[0]) == set(e["id"] for e in embeddings[1:])

    # Delete is idempotent
    if isinstance(segment, PersistentLocalHnswSegment):
        assert os.path.exists(segment._get_storage_folder())
        segment.delete()
        assert not os.path.exists(segment._get_storage_folder())
        segment.delete()  # should not raise
    elif isinstance(segment, LocalHnswSegment):
        with pytest.raises(NotImplementedError):
            segment.delete()


def test_reset_state_ignored_for_allow_reset_false(
    system: System,
    sample_embeddings: Iterator[SubmitEmbeddingRecord],
    vector_reader: Type[VectorReader],
    produce_fns: ProducerFn,
) -> None:
    producer = system.instance(Producer)
    system.reset_state()
    segment_definition = create_random_segment_definition()
    topic = str(segment_definition["topic"])

    segment = vector_reader(system, segment_definition)
    segment.start()

    embeddings, seq_ids = produce_fns(
        producer=producer, topic=topic, embeddings=sample_embeddings, n=5
    )

    sync(segment, seq_ids[-1])
    assert segment.count() == 5

    delete_record = SubmitEmbeddingRecord(
        id=embeddings[0]["id"],
        embedding=None,
        encoding=None,
        metadata=None,
        operation=Operation.DELETE,
        collection_id=uuid.UUID(int=0),
    )
    assert isinstance(seq_ids, List)
    seq_ids.append(
        produce_fns(
            producer=producer,
            topic=topic,
            n=1,
            embeddings=(delete_record for _ in range(1)),
        )[1][0]
    )

    sync(segment, seq_ids[-1])

    # Assert that the record is gone using `count`
    assert segment.count() == 4

    # Assert that the record is gone using `get`
    assert segment.get_vectors(ids=[embeddings[0]["id"]]) == []
    results = segment.get_vectors()
    assert len(results) == 4
    # get_vectors returns results in arbitrary order
    results = sorted(results, key=lambda v: v["id"])
    for actual, expected in zip(results, embeddings[1:]):
        assert actual["id"] == expected["id"]
        assert approx_equal_vector(
            actual["embedding"], cast(Vector, expected["embedding"])
        )

    # Assert that the record is gone from KNN search
    vector = cast(Vector, embeddings[0]["embedding"])
    query = VectorQuery(
        vectors=[vector], k=10, allowed_ids=None, options=None, include_embeddings=False
    )
    knn_results = segment.query_vectors(query)
    assert len(results) == 4
    assert set(r["id"] for r in knn_results[0]) == set(e["id"] for e in embeddings[1:])

    if isinstance(segment, PersistentLocalHnswSegment):
        if segment._allow_reset:
            assert os.path.exists(segment._get_storage_folder())
            segment.reset_state()
            assert not os.path.exists(segment._get_storage_folder())
        else:
            assert os.path.exists(segment._get_storage_folder())
            segment.reset_state()
            assert os.path.exists(segment._get_storage_folder())
