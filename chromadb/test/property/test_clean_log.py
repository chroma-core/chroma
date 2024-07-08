from typing import cast
from overrides import overrides
from chromadb.api import ServerAPI
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

    @invariant()
    def log_empty_after_cleaning(self) -> None:
        producer = self.api._system.instance(Producer)
        sqlite = self.api._system.instance(SqliteDB)

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


def test_wal_clean(api: ServerAPI) -> None:
    run_state_machine_as_test(
        lambda: LogCleanEmbeddingStateMachine(api=api),
    ) # type: ignore
