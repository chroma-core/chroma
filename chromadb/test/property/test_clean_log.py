from typing import Generator, cast
from chromadb.ingest import Producer
from overrides import overrides
import pytest
from chromadb.api.client import Client
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
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


# todo: needed?
collection_persistent_st = st.shared(
    # Set a small batch size, otherwise it's unlikely that .clean_log() will have any effect
    strategies.collections(
        with_hnsw_params=True,
        with_persistent_hnsw_params=st.just(True),
        max_hnsw_sync_threshold=5,
        max_hnsw_batch_size=5,
    ),
    key="coll_persistent",
)


def total_embedding_queue_log_size(sqlite: SqliteDB) -> int:
    t = Table("embeddings_queue")
    q = sqlite.querybuilder().from_(t)

    with sqlite.tx() as cur:
        sql, params = get_sql(
            q.select(functions.Count(t.seq_id)), sqlite.parameter_format()
        )
        result = cur.execute(sql, params)
        return cast(int, result.fetchone()[0])


# todo: combine with RestartablePersistedEmbeddingStateMachine
class LogCleanEmbeddingStateMachine(EmbeddingStateMachineBase):
    has_collection_mutated = False
    system: System

    def __init__(self, system: System) -> None:
        self.system = system
        client = Client.from_system(system)
        super().__init__(client)

    @rule()
    def log_empty_after_cleaning(self) -> None:
        producer = self.system.instance(Producer)
        sqlite = self.system.instance(SqliteDB)

        producer.clean_log(self.collection.id)

        if self.has_collection_mutated:
            # Must always keep one entry to avoid reusing seq_ids
            assert total_embedding_queue_log_size(sqlite) >= 1

            if self.system.settings.is_persistent:
                sync_threshold = self.collection.metadata.get("hnsw:sync_threshold", -1)
                batch_size = self.collection.metadata.get("hnsw:batch_size", -1)

                # -1 is used because the queue is always at least 1 entry long, so deletion stops before the max ack'ed sequence ID.
                # And if the batch_size != sync_threshold, the queue can have up to batch_size - 1 more entries.
                assert (
                    total_embedding_queue_log_size(sqlite) - 1
                    <= sync_threshold + batch_size - 1
                )
            else:
                assert total_embedding_queue_log_size(sqlite) <= 1
        else:
            assert total_embedding_queue_log_size(sqlite) == 0

    @overrides
    def on_state_change(self, new_state: str) -> None:
        if new_state != EmbeddingStateMachineStates.initialize:
            self.has_collection_mutated = True


# This machine shares a lot of similarity with the machine in chromadb/test/property/test_persist.py, but it's a separate machine because test_persist makes assertions
class PersistentLogCleanEmbeddingStateMachine(LogCleanEmbeddingStateMachine):
    @initialize(collection=collection_persistent_st)  # type: ignore
    @overrides
    def initialize(self, collection: strategies.Collection):
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

    @rule()
    def restart_system(self) -> None:
        # Simulates restarting chromadb
        # (there's some edge cases around correctly tracking sequence IDs at client startup)
        self.system.stop()
        self.system = System(self.system.settings)
        self.system.start()
        self.client.clear_system_cache()
        self.client = Client.from_system(self.system)
        self.collection = self.client.get_collection(
            self.collection.name, embedding_function=self.embedding_function
        )

    @overrides
    def teardown(self) -> None:
        super().teardown()
        # Need to manually stop the system to cleanup resources because we may have created a new system (above rule).
        # Normally, we wouldn't have to worry about this as the system from the fixture is shared between state machine runs.
        # (This helps avoid a "too many open files" error.)
        self.system.stop()


@pytest.fixture(params=[sqlite_fixture, sqlite_persistent_fixture])
def any_sqlite(request: pytest.FixtureRequest) -> Generator[System, None, None]:
    yield from request.param()


def test_clean_log(any_sqlite: System) -> None:
    if any_sqlite.settings.is_persistent:
        run_state_machine_as_test(
            lambda: PersistentLogCleanEmbeddingStateMachine(any_sqlite),
        )  # type: ignore
    else:
        run_state_machine_as_test(
            lambda: LogCleanEmbeddingStateMachine(any_sqlite),
        )  # type: ignore
