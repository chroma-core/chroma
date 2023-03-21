import chromadb.db.migrations
from chromadb.db.migrations import MigratableDB
from chromadb.db import Segment, SysDB
from chromadb.config import Settings
import duckdb
import os


class TxWrapper(chromadb.db.migrations.TxWrapper):
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


class DuckDB2(MigratableDB, SysDB):
    def __init__(self, settings: Settings):
        settings.validate("duckdb_database")
        self._conn = duckdb.connect(database=settings.duckdb_database)  # type: ignore
        self._settings = settings

    def tx(self):
        return TxWrapper(self._conn)  # type: ignore

    @staticmethod
    def migration_dirs():
        return ["migrations/sysdb"]

    @staticmethod
    def migration_scope():
        return "duckdb"

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

    def create_segment(self, segment):
        raise NotImplementedError()

    def get_segments(self, id, embedding_function, metadata):
        raise NotImplementedError()
