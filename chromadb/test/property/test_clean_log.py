from typing import Generator, cast
from chromadb.ingest import Producer
from overrides import overrides
import pytest
from chromadb.api.client import Client
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from pypika import Table, functions
from hypothesis.stateful import (
    rule,
    run_state_machine_as_test,
)

from chromadb.test.conftest import sqlite_fixture, sqlite_persistent_fixture
from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachineBase,
    EmbeddingStateMachineStates,
)
from chromadb.test.property.test_restart_persist import (
    RestartablePersistedEmbeddingStateMachine,
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


class PersistentLogCleanEmbeddingStateMachine(
    LogCleanEmbeddingStateMachine, RestartablePersistedEmbeddingStateMachine
):
    ...


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
