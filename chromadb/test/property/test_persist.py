import logging
from typing import Generator, Callable
from hypothesis import given
import hypothesis.strategies as st
import pytest
import chromadb
from chromadb.api import ClientAPI, ServerAPI
from chromadb.config import Settings, System
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
import os
import shutil
import tempfile

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
    api_1 = system_1.instance(ServerAPI)
    system_1.start()

    api_1.reset()
    coll = api_1.create_collection(
        name=collection_strategy.name,
        metadata=collection_strategy.metadata,
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
    del api_1
    del system_1

    system_2 = System(settings)
    api_2 = system_2.instance(ServerAPI)
    system_2.start()

    coll = api_2.get_collection(
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
    del api_2
    del system_2


def load_and_check(
    settings: Settings,
    collection_name: str,
    record_set: strategies.RecordSet,
) -> None:
    system = System(settings)
    api = system.instance(ServerAPI)
    system.start()

    coll = api.get_collection(
        name=collection_name,
        embedding_function=strategies.not_implemented_embedding_function(),
    )
    invariants.count(coll, record_set)
    invariants.metadatas_match(coll, record_set)
    invariants.documents_match(coll, record_set)
    invariants.ids_match(coll, record_set)
    invariants.ann_accuracy(coll, record_set)

    system.stop()


class PersistEmbeddingsStateMachineStates(EmbeddingStateMachineStates):
    persist = "persist"


class PersistEmbeddingsStateMachine(EmbeddingStateMachine):
    def __init__(self, api: ClientAPI, settings: Settings):
        self.api = api
        self.settings = settings
        self.last_persist_delay = 10
        self.api.reset()
        super().__init__(self.api)

    @initialize(collection=embedding_collection_st, batch_size=st.integers(min_value=3, max_value=2000), sync_threshold=st.integers(min_value=3, max_value=2000))  # type: ignore
    def initialize(
        self, collection: strategies.Collection, batch_size: int, sync_threshold: int
    ):
        self.api.reset()
        self.collection = self.api.create_collection(
            name=collection.name,
            metadata=collection.metadata,
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

        load_and_check(self.settings, collection_name, self.record_set_state)  # type: ignore

    def on_state_change(self, new_state: str) -> None:
        if new_state == PersistEmbeddingsStateMachineStates.persist:
            self.last_persist_delay = 10
        else:
            self.last_persist_delay -= 1

    def teardown(self) -> None:
        self.api.reset()


def test_persist_embeddings_state(
    caplog: pytest.LogCaptureFixture, settings: Settings
) -> None:
    caplog.set_level(logging.ERROR)
    api = chromadb.Client(settings)
    run_state_machine_as_test(
        lambda: PersistEmbeddingsStateMachine(settings=settings, api=api)
    )  # type: ignore
