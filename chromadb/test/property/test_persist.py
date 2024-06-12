import logging
import multiprocessing
from multiprocessing.connection import Connection
from typing import Generator, Callable
from hypothesis import given
import hypothesis.strategies as st
import pytest
import chromadb
from chromadb.api import ClientAPI, ServerAPI
from chromadb.config import Settings, System
from chromadb.test.conftest import override_hypothesis_profile
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachine,
    EmbeddingStateMachineStates,
    collection_st as embedding_collection_st,
    trace,
)
from hypothesis.stateful import (
    run_state_machine_as_test,
    rule,
    precondition,
    initialize,
)
import hypothesis
import os
import shutil
import tempfile
from chromadb.api.client import Client as ClientCreator

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
    strategies.collections(with_hnsw_params=True, with_persistent_hnsw_params=True),
    key="coll",
)


@given(
    collection_strategy=collection_st,
    embeddings_strategy=strategies.recordsets(collection_st),
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
        metadata=collection_strategy.metadata,  # type: ignore
        embedding_function=collection_strategy.embedding_function,
    )

    if not invariants.is_metadata_valid(invariants.wrap_all(embeddings_strategy)):
        with pytest.raises(Exception):
            coll.add(**embeddings_strategy)
        return

    coll.add(**embeddings_strategy)

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


def get_multiprocessing_context():
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


class PersistEmbeddingsStateMachine(EmbeddingStateMachine):
    def __init__(self, client: ClientAPI, settings: Settings):
        self.client = client
        self.settings = settings
        self.last_persist_delay = 10
        self.client.reset()
        super().__init__(self.client)

    @initialize(collection=embedding_collection_st, batch_size=st.integers(min_value=3, max_value=2000), sync_threshold=st.integers(min_value=3, max_value=2000))  # type: ignore
    def initialize(
        self, collection: strategies.Collection, batch_size: int, sync_threshold: int
    ):
        self.client.reset()
        self.collection = self.client.create_collection(
            name=collection.name,
            metadata=collection.metadata,  # type: ignore
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
        and self.last_persist_delay <= 0
    )
    @rule()
    def persist(self) -> None:
        self.on_state_change(PersistEmbeddingsStateMachineStates.persist)
        collection_name = self.collection.name
        conn1, conn2 = multiprocessing.Pipe()
        ctx = get_multiprocessing_context()
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
        if new_state == PersistEmbeddingsStateMachineStates.persist:
            self.last_persist_delay = 10
        else:
            self.last_persist_delay -= 1

    def teardown(self) -> None:
        self.client.reset()


def test_persist_embeddings_state(
    caplog: pytest.LogCaptureFixture, settings: Settings
) -> None:
    caplog.set_level(logging.ERROR)
    client = chromadb.Client(settings)
    run_state_machine_as_test(
        lambda: PersistEmbeddingsStateMachine(settings=settings, client=client),
        # For small max_example values, the test may not generate any examples that pass the precondition for persist().
        # This value makes it much more likely that the precondition will be satisfied and thus the rule will be exercised.
        _min_steps=10,
        settings=override_hypothesis_profile(fast=hypothesis.settings(max_examples=10)),
    )  # type: ignore
