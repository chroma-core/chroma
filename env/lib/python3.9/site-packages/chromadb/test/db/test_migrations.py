import pytest
from importlib_resources import files
from typing import Generator, List, Callable
import chromadb.db.migrations as migrations
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings
from pytest import FixtureRequest
import copy


def sqlite() -> Generator[migrations.MigratableDB, None, None]:
    """Fixture generator for sqlite DB"""
    db = SqliteDB(
        System(
            Settings(
                migrations="none",
                allow_reset=True,
            )
        )
    )
    db.start()
    yield db


def db_fixtures() -> List[Callable[[], Generator[migrations.MigratableDB, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=db_fixtures())
def db(request: FixtureRequest) -> Generator[migrations.MigratableDB, None, None]:
    yield next(request.param())


# Some Database impls improperly swallow exceptions, test that the wrapper works
def test_exception_propagation(db: migrations.MigratableDB) -> None:
    with pytest.raises(Exception):
        with db.tx():
            raise (Exception("test exception"))


def test_setup_migrations(db: migrations.MigratableDB) -> None:
    db.reset_state()
    db.setup_migrations()
    db.setup_migrations()  # idempotent

    with db.tx() as cursor:
        rows = cursor.execute("SELECT * FROM migrations").fetchall()
        assert len(rows) == 0


def test_migrations(db: migrations.MigratableDB) -> None:
    db.initialize_migrations()

    dir = files("chromadb.test.db.migrations")
    db_migrations = db.db_migrations(dir)
    source_migrations = migrations.find_migrations(dir, db.migration_scope())

    unapplied_migrations = migrations.verify_migration_sequence(
        db_migrations, source_migrations
    )

    assert unapplied_migrations == source_migrations

    with db.tx() as cur:
        rows = cur.execute("SELECT * FROM migrations").fetchall()
        assert len(rows) == 0

    with db.tx() as cur:
        for m in unapplied_migrations[:-1]:
            db.apply_migration(cur, m)

    db_migrations = db.db_migrations(dir)
    unapplied_migrations = migrations.verify_migration_sequence(
        db_migrations, source_migrations
    )

    assert len(unapplied_migrations) == 1
    assert unapplied_migrations[0]["version"] == 3

    with db.tx() as cur:
        assert len(cur.execute("SELECT * FROM migrations").fetchall()) == 2
        assert len(cur.execute("SELECT * FROM table1").fetchall()) == 0
        assert len(cur.execute("SELECT * FROM table2").fetchall()) == 0
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM table3").fetchall()

    with db.tx() as cur:
        for m in unapplied_migrations:
            db.apply_migration(cur, m)

    db_migrations = db.db_migrations(dir)
    unapplied_migrations = migrations.verify_migration_sequence(
        db_migrations, source_migrations
    )

    assert len(unapplied_migrations) == 0

    with db.tx() as cur:
        assert len(cur.execute("SELECT * FROM migrations").fetchall()) == 3
        assert len(cur.execute("SELECT * FROM table3").fetchall()) == 0


def test_tampered_migration(db: migrations.MigratableDB) -> None:
    db.reset_state()

    db.setup_migrations()

    dir = files("chromadb.test.db.migrations")
    source_migrations = migrations.find_migrations(dir, db.migration_scope())

    db_migrations = db.db_migrations(dir)

    unapplied_migrations = migrations.verify_migration_sequence(
        db_migrations, source_migrations
    )

    with db.tx() as cur:
        for m in unapplied_migrations:
            db.apply_migration(cur, m)

    db_migrations = db.db_migrations(dir)
    unapplied_migrations = migrations.verify_migration_sequence(
        db_migrations, source_migrations
    )
    assert len(unapplied_migrations) == 0

    inconsistent_version_migrations = copy.deepcopy(source_migrations)
    inconsistent_version_migrations[0]["version"] = 2

    with pytest.raises(migrations.InconsistentVersionError):
        migrations.verify_migration_sequence(
            db_migrations, inconsistent_version_migrations
        )

    inconsistent_hash_migrations = copy.deepcopy(source_migrations)
    inconsistent_hash_migrations[0]["hash"] = "badhash"

    with pytest.raises(migrations.InconsistentHashError):
        migrations.verify_migration_sequence(
            db_migrations, inconsistent_hash_migrations
        )


def test_initialization(
    monkeypatch: pytest.MonkeyPatch, db: migrations.MigratableDB
) -> None:
    db.reset_state()
    dir = files("chromadb.test.db.migrations")
    monkeypatch.setattr(db, "migration_dirs", lambda: [dir])

    assert not db.migrations_initialized()

    with pytest.raises(migrations.UninitializedMigrationsError):
        db.validate_migrations()

    db.setup_migrations()

    assert db.migrations_initialized()

    with pytest.raises(migrations.UnappliedMigrationsError):
        db.validate_migrations()

    db.apply_migrations()
    db.validate_migrations()
