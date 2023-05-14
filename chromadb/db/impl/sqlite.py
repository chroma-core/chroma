from chromadb.db.migrations import MigratableDB, Migration
from chromadb.config import Settings
import chromadb.db.base as base
import sqlite3
from overrides import override
import pypika
from typing import Sequence, cast, Optional, Type, Literal
from types import TracebackType


class TxWrapper(base.TxWrapper):
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    @override
    def __enter__(self) -> base.Cursor:
        self._conn.execute("BEGIN;")
        return self._conn.cursor()  # type: ignore

    @override
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        if exc_type is None:
            self._conn.commit()
        else:
            self._conn.rollback()
        return False


class SqliteDB(MigratableDB):
    _conn: sqlite3.Connection
    _settings: Settings

    def __init__(self, settings: Settings):
        self._settings = settings
        self._init()
        super().__init__(settings)

    def _init(self) -> None:
        sqlite_db = self._settings.require("sqlite_database")
        self._conn = sqlite3.connect(sqlite_db)
        self.initialize_migrations()

    @staticmethod
    @override
    def querybuilder() -> type[pypika.Query]:
        return pypika.Query  # type: ignore

    @staticmethod
    @override
    def parameter_format() -> str:
        return "?"

    @staticmethod
    @override
    def migration_dirs() -> Sequence[str]:
        return []

    @staticmethod
    @override
    def migration_scope() -> str:
        return "sqlite"

    @override
    def tx(self) -> TxWrapper:
        return TxWrapper(self._conn)

    @override
    def reset(self) -> None:
        self._conn.close()
        self._init()

    @override
    def setup_migrations(self) -> None:
        with self.tx() as cur:
            cur.execute(
                """
                 CREATE TABLE IF NOT EXISTS migrations (
                     dir TEXT NOT NULL,
                     version INTEGER NOT NULL,
                     filename TEXT NOT NULL,
                     sql TEXT NOT NULL,
                     hash TEXT NOT NULL,
                     PRIMARY KEY (dir, version)
                 )
                 """
            )

    @override
    def migrations_initialized(self) -> bool:
        with self.tx() as cur:
            cur.execute(
                """SELECT count(*) FROM sqlite_master
                   WHERE type='table' AND name='migrations'"""
            )

            if cur.fetchone()[0] == 0:
                return False
            else:
                return True

    @override
    def db_migrations(self, dir: str) -> Sequence[Migration]:
        with self.tx() as cur:
            cur.execute(
                """
                WHERE dir = ?
                ORDER BY version ASC
                """,
                (dir,),
            )

            migrations = []
            for row in cur.fetchall():
                dir = cast(str, row[0])
                version = cast(int, row[1])
                filename = cast(str, row[2])
                sql = cast(str, row[3])
                hash = cast(str, row[4])
                migrations.append(
                    Migration(
                        dir=dir,
                        version=version,
                        filename=filename,
                        sql=sql,
                        hash=hash,
                        scope=self.migration_scope(),
                    )
                )
            return migrations

    @override
    def apply(self, cur: base.Cursor, migration: Migration) -> None:
        cur.execute(migration["sql"])
        cur.execute(
            """
            INSERT INTO migrations (dir, version, filename, sql, hash)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                migration["dir"],
                migration["version"],
                migration["filename"],
                migration["sql"],
                migration["hash"],
            ),
        )
