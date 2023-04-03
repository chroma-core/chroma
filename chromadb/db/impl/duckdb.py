import chromadb.db.migrations as migrations
from chromadb.db.migrations import MigratableDB
from chromadb.config import Settings
from chromadb.db.basesqlsysdb import BaseSqlSysDB
from chromadb.ingest import Producer, Consumer, get_encoding, encode_vector
from chromadb.types import (
    InsertEmbeddingRecord,
    Where,
    WhereDocument,
    MetadataEmbeddingRecord,
)
import pypika
import duckdb
from pubsub import pub
from typing import Sequence, Optional, cast
from pypika import Table
import json
from overrides import override


class TxWrapper(migrations.TxWrapper):
    def __init__(self, conn):
        self._conn = conn.cursor()

    @override
    def __enter__(self):
        self._conn.begin()
        return self._conn

    @override
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
            return False


class DuckDB(MigratableDB, BaseSqlSysDB, Producer, Consumer):
    def __init__(self, settings: Settings):
        settings.validate("duckdb_database")
        settings.validate("migrations")
        self._conn = duckdb.connect(database=settings.duckdb_database)  # type: ignore

        self._settings = settings

        if settings.migrations == "validate":
            self.validate_migrations()

        if settings.migrations == "apply":
            self.apply_migrations()

    @override
    def tx(self) -> TxWrapper:
        return TxWrapper(self._conn)  # type: ignore

    @staticmethod
    @override
    def querybuilder() -> type[pypika.Query]:
        return pypika.Query

    @staticmethod
    @override
    def parameter_format() -> str:
        return "?"

    @staticmethod
    @override
    def migration_dirs() -> Sequence[str]:
        return ["migrations/sysdb", "migrations/ingestdb"]

    @staticmethod
    @override
    def migration_scope() -> str:
        return "duckdb"

    @override
    def delete_topic(self, topic_name: str):
        with self.tx() as cur:
            cur.execute(
                """
                DELETE FROM embeddings
                WHERE topic = ?
                """,
                (topic_name,),
            )
            cur.execute(
                """
                DELETE FROM embedding_metadata
                WHERE topic = ?
                """,
                (topic_name,),
            )

    @override
    def submit_embedding(self, topic_name: str, embedding: InsertEmbeddingRecord):
        encoding = get_encoding(embedding)
        vector = encode_vector(embedding["embedding"], encoding)

        embedding_record = {**embedding}

        metadata = None
        if embedding["metadata"] is not None and len(embedding["metadata"]) > 0:
            metadata = json.dumps(embedding["metadata"])

        with self.tx() as cur:
            cur.execute(
                """
                INSERT INTO embeddings (topic, id, encoding, vector, metadata)
                VALUES (?, ?, ?, ?, ?)
                RETURNING seq
                """,
                (topic_name, embedding["id"], encoding.value, vector, metadata),
            )
            seq_id = cur.fetchone()[0]
            embedding_record["seq_id"] = seq_id

            self._publish(embedding_record)

    def _publish(self, embedding_record):
        pass

    @override
    def submit_embedding_delete(self, topic_name: str, id: str) -> None:
        raise NotImplementedError()

    @override
    def register_consume_fn(self, topic_name, consume_fn, start=None, end=None):
        raise NotImplementedError()

    @override
    def setup_migrations(self):
        with self.tx() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS migrations (
                    dir TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    hash TEXT NOT NULL,
                    PRIMARY KEY (dir, version)
                )
                """
            )

    @override
    def migrations_initialized(self):
        with self.tx() as cur:
            try:
                cur.execute("SHOW TABLE migrations")
                return True
            except duckdb.CatalogException:
                return False

    @override
    def reset(self):
        self._conn.close()
        # TODO: If using a persistent connection, delete the perist file
        self._conn = duckdb.connect(database=self._settings["duckdb_database"])
        self.apply_migrations()

    def count_embeddings(self, topic_name: str) -> int:
        """Return the number of embeddings in a topic."""
        with self.tx() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM embeddings
                WHERE topic = ?
                """,
                (topic_name,),
            )
            return cur.fetchone()[0]

    def get_metadata(
        self,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        ids: Optional[Sequence[str]] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:

        table = Table("embeddings")

        query = self.querybuilder().from_(table)

        query = query.select(table.topic, table.id, table.seq, table.metadata)

        if where is not None:
            # TODO: Implement where-based filtering using json
            pass

    def _format_where(query, where):
        # TODO: Answer the question of how multityped data is to be saved. Separate columns? Jsonified?

        return query

    def _format_where_document(query, where_document):

        return query
