from typing import Generator, cast
from overrides import overrides
import pytest
from chromadb.api.client import Client
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.ingest import Producer
from pypika import Table, functions
import hypothesis.strategies as st
from hypothesis.stateful import (
    rule,
    run_state_machine_as_test,
    initialize,
)

from chromadb.test.conftest import sqlite_fixture, sqlite_persistent_fixture
from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachineBase,
    EmbeddingStateMachineStates,
    trace,
)
import chromadb.test.property.strategies as strategies

collection_st = st.shared(
    strategies.collections(with_hnsw_params=True, with_persistent_hnsw_params=True),
    key="coll",
)


def count_embedding_queue_rows(sqlite: SqliteDB) -> int:
    t = Table("embeddings_queue")
    q = sqlite.querybuilder().from_(t).select(functions.Count(t.seq_id))

    with sqlite.tx() as cur:
        sql, params = get_sql(q, sqlite.parameter_format())
        result = cur.execute(sql, params)
        return cast(int, result.fetchone()[0])


# Set a small batch size, otherwise it's unlikely that .clean_log() will have any effect
HNSW_BATCH_SIZE = 3


class LogCleanEmbeddingStateMachine(EmbeddingStateMachineBase):
    has_collection_mutated = False
    system: System

    def __init__(self, system: System) -> None:
        self.system = system
        client = Client.from_system(system)
        super().__init__(client)

    # Override to set the batch size
    @initialize(collection=collection_st)  # type: ignore
    @overrides
    def initialize(self, collection: strategies.Collection):
        self.client.reset()

        collection.metadata["hnsw:batch_size"] = HNSW_BATCH_SIZE  # type: ignore

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

    @rule()
    def log_empty_after_cleaning(self) -> None:
        producer = self.system.instance(Producer)
        sqlite = self.system.instance(SqliteDB)

        producer.clean_log(self.collection.id)
        num_rows = count_embedding_queue_rows(sqlite)

        if self.has_collection_mutated:
            # Must always keep one entry to avoid reusing seq_ids
            assert num_rows >= 1

            batch_size = self.collection.metadata.get("hnsw:batch_size")
            assert batch_size == HNSW_BATCH_SIZE
            assert num_rows <= batch_size
        else:
            assert num_rows == 0

    @overrides
    def on_state_change(self, new_state: str) -> None:
        if new_state != EmbeddingStateMachineStates.initialize:
            self.has_collection_mutated = True


class PersistentLogCleanEmbeddingStateMachine(LogCleanEmbeddingStateMachine):
    @rule()
    def restart_system(self) -> None:
        # Simulates restarting chromadb
        # (there's some edge cases around correctly tracking sequence IDs at client startup)
        self.system.stop()
        self.system = System(self.system.settings)
        self.system.start()
        self.client.clear_system_cache()
        self.client = Client.from_system(self.system)
        self.collection = self.client.get_collection(self.collection.name)


@pytest.fixture(params=[sqlite_fixture, sqlite_persistent_fixture])
def any_sqlite(request: pytest.FixtureRequest) -> Generator[System, None, None]:
    yield from request.param()


def test_clean_log(any_sqlite: System) -> None:
    run_state_machine_as_test(
        lambda: LogCleanEmbeddingStateMachine(any_sqlite),
    )  # type: ignore


def test_cleanup_after_system_restart(sqlite_persistent: System) -> None:
    run_state_machine_as_test(
        lambda: PersistentLogCleanEmbeddingStateMachine(sqlite_persistent),
    )  # type: ignore
