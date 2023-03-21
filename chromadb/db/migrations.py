from abc import ABC, abstractmethod
from typing import TypedDict, Sequence
import os
import re
import hashlib


class MigrationFile(TypedDict):
    dir: str
    filename: str
    version: int
    scope: str


class Migration(MigrationFile):
    hash: str
    sql: str


class InconsistentVersionError(Exception):
    pass


class InconsistentHashError(Exception):
    pass


class TxWrapper(ABC):
    """Wrapper class for DBAPI 2.0 Connection objects, with which clients can implement transactions.
    Makes two guarantees that basic DBAPI 2.0 connections do not:

    - __enter__ returns a Cursor object consistently (instead of a Connection like some do)
    - Always re-raises an exception if one was thrown from the body
    """

    @abstractmethod
    def __enter__(self) -> object:
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_value, traceback):
        pass


# TODO: SIMPLIFY, refactor to functions and make MigratableDB a mixin


class MigratableDB(ABC):
    """Simple base class for databases which support basic migrations.

    Migrations are SQL files stored in a project-relative directory. All migrations in the
    same directory are assumed to be dependent on other migrations from the same directory.

    Migrations have a ascending numeric version number and a hash of the file contents. When migrations are applied,
    the hashes of previous migrations are checked to ensure that the database is consistent with the source repository.
    If they are not, an error is thrown and no migrations will be applied.

    Migration files must follow the naming convention: <version>.<description>.<scope>.sql, where <version> is a
    5-digit zero-padded integer, <description> is a short textual description, and <scope> is a short string
    identifying the database implementation.
    """

    @staticmethod
    @abstractmethod
    def migration_dirs() -> Sequence[str]:
        """Directories containing the migration sequences that should be applied to this DB."""
        pass

    @staticmethod
    @abstractmethod
    def migration_scope() -> str:
        """The database implementation to use for migrations (e.g, sqlite, pgsql)"""
        pass

    @abstractmethod
    def tx(self) -> TxWrapper:
        """Return a TxWrapper for transactions"""
        pass

    @abstractmethod
    def setup_migrations(self):
        """Apply migration 0, which idempotently creates the migrations table"""
        pass

    def apply_all_migrations(self):
        """Apply all migrations in the given directories, in ascending order. Throw an exception if any migrations
        have already been applied, or if the database is inconsistent with the source code."""
        for dir in self.migration_dirs():
            with self.tx() as cur:
                migrations = source_migrations(dir, self.migration_scope())
                unapplied_migrations = validate_migrations(cur, migrations)
                for migration in unapplied_migrations:
                    apply_migration(cur, migration)


filename_regex = re.compile(r"(\d+)-(.+)\.(.+)\.sql")


def parse_migration_filename(dir, filename) -> MigrationFile:
    """Parse a migration filename into a MigrationFile object"""
    match = filename_regex.match(filename)
    if match is None:
        raise Exception("Invalid migration filename: " + filename)
    version, _, scope = match.groups()
    return {
        "dir": dir,
        "filename": filename,
        "version": int(version),
        "scope": scope,
    }


def read_migration_file(file: MigrationFile) -> Migration:
    """Read a migration file"""
    sql = open(os.path.join(file["dir"], file["filename"])).read()
    hash = hashlib.md5(sql.encode("utf-8")).hexdigest()
    return {**file, "hash": hash, "sql": sql}


def source_migrations(dir, scope):
    """Return a list of all migration present in the given directory, in ascending order. Filter by scope."""
    files = [
        parse_migration_filename(dir, filename)
        for filename in os.listdir(dir)
        if filename.endswith(".sql")
    ]
    files = filter(lambda f: f["scope"] == scope, files)
    files = sorted(files, key=lambda f: f["version"])
    return [read_migration_file(f) for f in files]


def validate_migrations(cur, migrations: Sequence[Migration]) -> Sequence[Migration]:
    """Validate that the given migration sequence is consistent with the database. Return all unapplied migrations,
    or an empty list if all migrations have been applied. Throw an exception if the database is inconsistent with
    the source code."""

    dir = migrations[0]["dir"]
    rows = cur.execute(
        "SELECT version, hash FROM migrations WHERE dir = ? ORDER BY version ASC", (dir,)
    ).fetchall()

    for row, migration in zip(rows, migrations):
        if row[0] != migration["version"]:
            raise InconsistentVersionError(
                f"Inconsistent migration versions in {dir}: {row[0]} != {migration['version']}"
            )
        if row[1] != migration["hash"]:
            raise InconsistentHashError(
                f"Inconsistent migration hashes for {migration['filename']}"
            )

    return migrations[len(rows) :]


def apply_migration(cur, migration: Migration):
    """Apply a single migration"""

    cur.execute(migration["sql"])
    cur.execute(
        "INSERT INTO migrations VALUES (?, ?, ?)",
        (migration["dir"], migration["version"], migration["hash"]),
    )
