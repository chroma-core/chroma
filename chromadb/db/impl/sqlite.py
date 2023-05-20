from chromadb.db.migrations import MigratableDB, Migration
from chromadb.config import System, Settings
import chromadb.db.base as base
import sqlite3
from overrides import override
import pypika
from typing import Sequence, cast, Optional, Type
from typing_extensions import Literal
from types import TracebackType
import os


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
    _migration_dirs: Sequence[str]

    def __init__(self, system: System):
        self._settings = system.settings
        self._migration_dirs = []
        self._init()
        super().__init__(system)

    def _init(self) -> None:
        sqlite_db = self._settings.require("sqlite_database")
        self._conn = sqlite3.connect(sqlite_db)
        self.initialize_migrations()

    @staticmethod
    @override
    def querybuilder() -> Type[pypika.Query]:
        return pypika.Query  # type: ignore

    @staticmethod
    @override
    def parameter_format() -> str:
        return "?"

    @staticmethod
    @override
    def migration_scope() -> str:
        return "sqlite"

    @override
    def migration_dirs(self) -> Sequence[str]:
        return self._migration_dirs

    @override
    def tx(self) -> TxWrapper:
        return TxWrapper(self._conn)

    @override
    def reset(self) -> None:
        if not self._settings.require("allow_reset"):
            raise ValueError(
                "Resetting the database is not allowed. Set `allow_reset` to true in the config in tests or other non-production environments where reset should be permitted."
            )
        self._conn.close()
        db_file = self._settings.require("sqlite_database")
        if db_file != ":memory:":
            os.remove(db_file)
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
                SELECT dir, version, filename, sql, hash
                FROM migrations
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
    def apply_migration(self, cur: base.Cursor, migration: Migration) -> None:
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
