import logging
import multiprocessing
from multiprocessing.connection import Connection
import multiprocessing.context
import time
from typing import Generator, Callable
from uuid import UUID
from hypothesis import given
import hypothesis.strategies as st
import pytest
import chromadb
from chromadb.api import ClientAPI, ServerAPI
from chromadb.config import Settings, System
from chromadb.segment import SegmentManager, VectorReader
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachineStates,
    trace,
    EmbeddingStateMachineBase,
)
from hypothesis.stateful import (
    run_state_machine_as_test,
    rule,
    precondition,
    initialize,
)
import os
import shutil
import tempfile
from chromadb.api.client import Client as ClientCreator
from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

CreatePersistAPI = Callable[[], ServerAPI]

configurations = [
    Settings(
        chroma_api_impl="chromadb.api.segment.SegmentAPI",
        chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
        chroma_segment_manager_impl="chromadb.segment.impl.manager.local.LocalSegmentManager",
        allow_reset=True,
        is_persistent=True,
        persist_directory=tempfile.mkdtemp(),
    ),
]


@pytest.fixture(scope="module", params=configurations)
def settings(request: pytest.FixtureRequest) -> Generator[Settings, None, None]:
    configuration = request.param
    save_path = configuration.persist_directory
    # Create if it doesn't exist
    if not os.path.exists(save_path):
        os.makedirs(save_path, exist_ok=True)
    yield configuration
    # Remove if it exists
    if os.path.exists(save_path):
        shutil.rmtree(save_path, ignore_errors=True)


collection_st = st.shared(
    strategies.collections(
        with_hnsw_params=True,
        with_persistent_hnsw_params=st.just(True),
        # Makes it more likely to find persist-related bugs (by default these are set to 2000).
        max_hnsw_batch_size=10,
        max_hnsw_sync_threshold=10,
    ),
    key="coll",
)


@given(
    collection_strategy=collection_st,
    embeddings_strategy=strategies.recordsets(collection_st, min_size=0),
)
def test_persist(
    settings: Settings,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.RecordSet,
) -> None:
    system_1 = System(settings)
    system_1.start()
    client_1 = ClientCreator.from_system(system_1)

    client_1.reset()
    coll = client_1.create_collection(
        name=collection_strategy.name,
        metadata=collection_strategy.metadata,  # type: ignore[arg-type]
        embedding_function=collection_strategy.embedding_function,
    )

    result = coll.add(**embeddings_strategy)  # type: ignore[arg-type]
    embeddings_strategy["ids"] = result["ids"]

    invariants.count(coll, embeddings_strategy)
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(
        coll,
        embeddings_strategy,
        embedding_function=collection_strategy.embedding_function,
    )

    system_1.stop()
    del client_1
    del system_1

    system_2 = System(settings)
    system_2.start()
    client_2 = ClientCreator.from_system(system_2)

    coll = client_2.get_collection(
        name=collection_strategy.name,
        embedding_function=collection_strategy.embedding_function,
    )
    invariants.count(coll, embeddings_strategy)
    invariants.metadatas_match(coll, embeddings_strategy)
    invariants.documents_match(coll, embeddings_strategy)
    invariants.ids_match(coll, embeddings_strategy)
    invariants.ann_accuracy(
        coll,
        embeddings_strategy,
        embedding_function=collection_strategy.embedding_function,
    )

    system_2.stop()
    del client_2
    del system_2


def test_sync_threshold(settings: Settings) -> None:
    system = System(settings)
    system.start()
    client = ClientCreator.from_system(system)

    collection = client.create_collection(
        name="test", metadata={"hnsw:batch_size": 3, "hnsw:sync_threshold": 3}
    )

    manager = system.instance(SegmentManager)
    segment = manager.get_segment(collection.id, VectorReader)

    def get_index_last_modified_at() -> float:
        # Time resolution on Windows can be up to 10ms
        time.sleep(0.1)
        try:
            return os.path.getmtime(segment._get_metadata_file())  # type: ignore[attr-defined]
        except FileNotFoundError:
            return -1

    last_modified_at = get_index_last_modified_at()

    collection.add(ids=["1", "2"], embeddings=[[1.0], [2.0]])  # type: ignore[arg-type]

    # Should not have yet persisted
    assert get_index_last_modified_at() == last_modified_at
    last_modified_at = get_index_last_modified_at()

    # Now there's 3 additions, and the sync threshold is 3...
    collection.add(ids=["3"], embeddings=[[3.0]])  # type: ignore[arg-type]

    # ...so it should have persisted
    assert get_index_last_modified_at() > last_modified_at
    last_modified_at = get_index_last_modified_at()

    # The same thing should happen with upserts
    collection.upsert(ids=["1", "2", "3"], embeddings=[[1.0], [2.0], [3.0]])  # type: ignore[arg-type]

    # Should have persisted
    assert get_index_last_modified_at() > last_modified_at
    last_modified_at = get_index_last_modified_at()

    # Mixed usage should also trigger persistence
    collection.add(ids=["4"], embeddings=[[4.0]])  # type: ignore[arg-type]
    collection.upsert(ids=["1", "2"], embeddings=[[1.0], [2.0]])  # type: ignore[arg-type]

    # Should have persisted
    assert get_index_last_modified_at() > last_modified_at
    last_modified_at = get_index_last_modified_at()

    # Invalid updates should also trigger persistence
    collection.add(ids=["5"], embeddings=[[5.0]])  # type: ignore[arg-type]
    collection.add(ids=["1", "2"], embeddings=[[1.0], [2.0]])  # type: ignore[arg-type]

    # Should have persisted
    assert get_index_last_modified_at() > last_modified_at
    last_modified_at = get_index_last_modified_at()


def load_and_check(
    settings: Settings,
    collection_name: str,
    record_set: strategies.RecordSet,
    conn: Connection,
) -> None:
    try:
        system = System(settings)
        system.start()
        client = ClientCreator.from_system(system)

        coll = client.get_collection(
            name=collection_name,
            embedding_function=strategies.not_implemented_embedding_function(),  # type: ignore[arg-type]
        )
        invariants.count(coll, record_set)
        invariants.metadatas_match(coll, record_set)
        invariants.documents_match(coll, record_set)
        invariants.ids_match(coll, record_set)
        invariants.ann_accuracy(coll, record_set)

        system.stop()
    except Exception as e:
        conn.send(e)
        raise e


def get_multiprocessing_context():  # type: ignore[no-untyped-def]
    try:
        # Run the invariants in a new process to bypass any shared state/caching (which would defeat the purpose of the test)
        # (forkserver is used because it's much faster than spawn—it will spawn a new, minimal singleton process and then fork that singleton)
        ctx = multiprocessing.get_context("forkserver")
        # This is like running `import chromadb` in the single process that is forked rather than importing it in each forked process.
        # Gives a ~3x speedup since importing chromadb is fairly expensive.
        ctx.set_forkserver_preload(["chromadb"])
        return ctx
    except Exception:
        # forkserver/fork is not available on Windows
        return multiprocessing.get_context("spawn")


class PersistEmbeddingsStateMachineStates(EmbeddingStateMachineStates):
    persist = "persist"


MIN_STATE_CHANGES_BEFORE_PERSIST = 5


class PersistEmbeddingsStateMachine(EmbeddingStateMachineBase):
    def __init__(self, client: ClientAPI, settings: Settings):
        self.client = client
        self.settings = settings
        self.min_state_changes_left_before_persisting = MIN_STATE_CHANGES_BEFORE_PERSIST
        self.client.reset()
        super().__init__(self.client)

    @initialize(collection=collection_st)  # type: ignore
    def initialize(self, collection: strategies.Collection):
        self.client.reset()
        self.collection = self.client.create_collection(
            name=collection.name,
            metadata=collection.metadata,  # type: ignore[arg-type]
            embedding_function=collection.embedding_function,
        )
        self.embedding_function = collection.embedding_function
        trace("init")
        self.on_state_change(EmbeddingStateMachineStates.initialize)

        self.record_set_state = strategies.StateMachineRecordSet(
            ids=[], metadatas=[], documents=[], embeddings=[]
        )

    @precondition(
        lambda self: len(self.record_set_state["ids"]) >= 1
        and self.min_state_changes_left_before_persisting <= 0
    )
    @rule()
    def persist(self) -> None:
        self.on_state_change(PersistEmbeddingsStateMachineStates.persist)
        collection_name = self.collection.name
        conn1, conn2 = multiprocessing.Pipe()
        ctx = get_multiprocessing_context()  # type: ignore[no-untyped-call]
        p = ctx.Process(
            target=load_and_check,
            args=(self.settings, collection_name, self.record_set_state, conn2),
        )
        p.start()
        p.join()

        if conn1.poll():
            e = conn1.recv()
            raise e

        p.close()

    def on_state_change(self, new_state: str) -> None:
        super().on_state_change(new_state)
        if new_state == PersistEmbeddingsStateMachineStates.persist:
            self.min_state_changes_left_before_persisting = (
                MIN_STATE_CHANGES_BEFORE_PERSIST
            )
        else:
            self.min_state_changes_left_before_persisting -= 1

    def teardown(self) -> None:
        self.client.reset()


def test_persist_embeddings_state(
    caplog: pytest.LogCaptureFixture, settings: Settings
) -> None:
    caplog.set_level(logging.ERROR)
    client = chromadb.Client(settings)
    run_state_machine_as_test(
        lambda: PersistEmbeddingsStateMachine(settings=settings, client=client),
    )  # type: ignore


# Ideally this scenario would be exercised by Hypothesis, but most runs don't seem to trigger this particular state.
def test_delete_add_after_persist(settings: Settings) -> None:
    client = chromadb.Client(settings)
    state = PersistEmbeddingsStateMachine(settings=settings, client=client)

    state.initialize(
        collection=strategies.Collection(
            name="A00",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
                # Important: both batch_size and sync_threshold are 3
                "hnsw:batch_size": 3,
                "hnsw:sync_threshold": 3,
            },
            embedding_function=DefaultEmbeddingFunction(),  # type: ignore[arg-type]
            id=UUID("0851f751-2f11-4424-ab23-4ae97074887a"),
            dimension=2,
            dtype=None,
            known_metadata_keys={},
            known_document_keywords=[],
            has_documents=False,
            has_embeddings=True,
        )
    )

    state.add_embeddings(
        record_set={
            # Add 3 records to hit the batch_size and sync_threshold
            "ids": ["0", "1", "2"],
            "embeddings": [[0, 0], [0, 0], [0, 0]],
            "metadatas": [None, None, None],
            "documents": None,
        }
    )

    # Delete and then re-add record
    state.delete_by_ids(ids=["0"])
    state.add_embeddings(
        record_set={
            "ids": ["0"],
            "embeddings": [[1, 1]],
            "metadatas": [None],
            "documents": None,
        }
    )

    # At this point, the changes above are not fully persisted
    state.fields_match()
