from typing import cast
from overrides import overrides
from chromadb.api import ServerAPI
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.ingest import Producer
from pypika import Table, functions
from hypothesis.stateful import (
    run_state_machine_as_test,
    invariant,
)

from chromadb.test.property.test_embeddings import (
    EmbeddingStateMachineBase,
    EmbeddingStateMachineStates,
)


def count_embedding_queue_rows(sqlite: SqliteDB) -> int:
    t = Table("embeddings_queue")
    q = sqlite.querybuilder().from_(t).select(functions.Count(t.seq_id))

    with sqlite.tx() as cur:
        sql, params = get_sql(q, sqlite.parameter_format())
        result = cur.execute(sql, params)
        return cast(int, result.fetchone()[0])


class LogCleanEmbeddingStateMachine(EmbeddingStateMachineBase):
    has_collection_mutated = False
    system: System

    def __init__(self, system: System) -> None:
        api = system.instance(ServerAPI)
        self.system = system
        super().__init__(api)

    @invariant()
    def log_empty_after_cleaning(self) -> None:
        producer = self.system.instance(Producer)
        sqlite = self.system.instance(SqliteDB)

        producer.clean_log(self.collection.id)
        num_rows = count_embedding_queue_rows(sqlite)

        if self.has_collection_mutated:
            # Must always keep one entry to avoid reusing seq_ids
            assert num_rows == 1
        else:
            assert num_rows == 0

    @overrides
    def on_state_change(self, new_state: str) -> None:
        if new_state != EmbeddingStateMachineStates.initialize:
            self.has_collection_mutated = True


def test_clean_log(sqlite_persistent: System) -> None:
    run_state_machine_as_test(
        lambda: LogCleanEmbeddingStateMachine(sqlite_persistent),
    )  # type: ignore


def test_cleanup_after_shutdown(sqlite_persistent: System) -> None:
    system = sqlite_persistent
    api = system.instance(ServerAPI)

    collection = api.create_collection("test")
    collection.add(["1", "2"], [[1.0], [1.0]])
    collection.add(["3", "4"], [[1.0], [1.0]])

    # Create new system to simulate a restart
    system.stop()

    system2 = System(system.settings)
    system2.start()

    producer = system2.instance(Producer)
    producer.clean_log(collection.id)

    assert count_embedding_queue_rows(system2.instance(SqliteDB)) == 1
