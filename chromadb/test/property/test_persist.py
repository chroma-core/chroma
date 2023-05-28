import logging
import multiprocessing
from multiprocessing.connection import Connection
from typing import Generator, Callable
from hypothesis import given
import hypothesis.strategies as st
import pytest
import chromadb
from chromadb.api import API
from chromadb.config import Settings
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachine,
    EmbeddingStateMachineStates,
)
from hypothesis.stateful import run_state_machine_as_test, rule, precondition
import os
import shutil
import tempfile

CreatePersistAPI = Callable[[], API]

configurations = [
    Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb+parquet",
        persist_directory=tempfile.gettempdir() + "/tests",
    )
]


@pytest.fixture(scope="module", params=configurations)
def settings(request: pytest.FixtureRequest) -> Generator[Settings, None, None]:
    configuration = request.param
    yield configuration
    save_path = configuration.persist_directory
    # Remove if it exists
    if os.path.exists(save_path):
        shutil.rmtree(save_path)


collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


@given(
    collection_strategy=collection_st,
    embeddings_strategy=strategies.recordsets(collection_st),
)
def test_persist(
    settings: Settings,
    collection_strategy: strategies.Collection,
    embeddings_strategy: strategies.RecordSet,
) -> None:
    api_1 = chromadb.Client(settings)
    api_1.reset()
    coll = api_1.create_collection(
        name=collection_strategy.name,
        metadata=collection_strategy.metadata,
        embedding_function=collection_strategy.embedding_function,
    )

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

    api_1.persist()
    del api_1

    api_2 = chromadb.Client(settings)
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


def load_and_check(
    settings: Settings,
    collection_name: str,
    record_set: strategies.RecordSet,
    conn: Connection,
) -> None:
    try:
        api = chromadb.Client(settings)
        coll = api.get_collection(
            name=collection_name,
            embedding_function=strategies.not_implemented_embedding_function(),
        )
        invariants.count(coll, record_set)
        invariants.metadatas_match(coll, record_set)
        invariants.documents_match(coll, record_set)
        invariants.ids_match(coll, record_set)
        invariants.ann_accuracy(coll, record_set)
    except Exception as e:
        conn.send(e)
        raise e


class PersistEmbeddingsStateMachineStates(EmbeddingStateMachineStates):
    persist = "persist"


class PersistEmbeddingsStateMachine(EmbeddingStateMachine):
    def __init__(self, api: API, settings: Settings):
        self.api = api
        self.settings = settings
        self.last_persist_delay = 10
        self.api.reset()
        super().__init__(self.api)

    @precondition(
        lambda self: len(self.record_set_state["ids"]) >= 1
        and self.last_persist_delay <= 0
    )
    @rule()
    def persist(self) -> None:
        self.on_state_change(PersistEmbeddingsStateMachineStates.persist)
        self.api.persist()
        collection_name = self.collection.name
        # Create a new process and then inside the process run the invariants
        # TODO: Once we switch off of duckdb and onto sqlite we can remove this
        ctx = multiprocessing.get_context("spawn")
        conn1, conn2 = multiprocessing.Pipe()
        p = ctx.Process(
            target=load_and_check,
            args=(self.settings, collection_name, self.record_set_state, conn2),
        )
        p.start()
        p.join()

        if conn1.poll():
            e = conn1.recv()
            raise e

    def on_state_change(self, new_state: str) -> None:
        if new_state == PersistEmbeddingsStateMachineStates.persist:
            self.last_persist_delay = 10
        else:
            self.last_persist_delay -= 1


def test_persist_embeddings_state(
    caplog: pytest.LogCaptureFixture, settings: Settings
) -> None:
    caplog.set_level(logging.ERROR)
    api = chromadb.Client(settings)
    run_state_machine_as_test(
        lambda: PersistEmbeddingsStateMachine(settings=settings, api=api)
    )  # type: ignore
