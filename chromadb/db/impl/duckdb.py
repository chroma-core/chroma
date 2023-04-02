import chromadb.db.migrations as migrations
from chromadb.db.migrations import MigratableDB
from chromadb.config import Settings
from chromadb.db.basesqlsysdb import BaseSqlSysDB
from chromadb.ingest import Producer, Consumer
from chromadb.types import InsertEmbeddingRecord
import pypika
import duckdb
from pubsub import pub


class TxWrapper(migrations.TxWrapper):
    def __init__(self, conn):
        self._conn = conn.cursor()

    def __enter__(self):
        self._conn.begin()
        return self._conn

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
        with self.tx() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS chroma")
            cur.execute("SET SCHEMA=chroma")

        self._settings = settings

        if settings.migrations == "validate":
            self.validate_migrations()

        if settings.migrations == "apply":
            self.apply_migrations()

    def tx(self):
        return TxWrapper(self._conn)  # type: ignore

    @staticmethod
    def querybuilder():
        return pypika.Query

    @staticmethod
    def parameter_format() -> str:
        return "?"

    @staticmethod
    def migration_dirs():
        return ["migrations/sysdb"]

    @staticmethod
    def migration_scope():
        return "duckdb"

    def delete_topic(self, topic_name: str):
        with self.tx() as cur:
            cur.execute(
                """
                DELETE FROM messages
                WHERE topic = ?
                """,
                (topic_name,),
            )

    def submit_embedding(self, topic_name: str, message: InsertEmbeddingRecord):
        raise NotImplementedError()

    def submit_embedding_delete(self, topic_name: str, id: str) -> None:
        raise NotImplementedError()

    def register_consume_fn(self, topic_name, consume_fn, start=None, end=None):
        raise NotImplementedError()

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

    def migrations_initialized(self):
        with self.tx() as cur:
            try:
                cur.execute("SHOW TABLE migrations")
                return True
            except duckdb.CatalogException:
                return False

    def reset(self):
        with self.tx() as cur:
            cur.execute("DROP SCHEMA chroma CASCADE")

        with self.tx() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS chroma")
            cur.execute("SET SCHEMA=chroma")

        self.apply_migrations()
