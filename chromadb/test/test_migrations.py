import chromadb.db.duckdb2
import chromadb.db.migrations as migrations
from chromadb.config import Settings
import pytest
import copy


@pytest.fixture
def duckdb_db():
    return chromadb.db.duckdb2.DuckDB2(Settings(duckdb_database=":memory:"))


test_dbs = [duckdb_db]

# Some Database impls improperly swallow exceptions, test that the wrapper works
@pytest.mark.parametrize("db_fixture", test_dbs)
def test_exception_propagation(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    with pytest.raises(Exception):
        with db.tx() as cursor:
            raise (Exception("test exception"))


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_setup_migrations(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    db.setup_migrations()
    db.setup_migrations()  # idempotent

    with db.tx() as cursor:
        rows = cursor.execute("SELECT * FROM migrations").fetchall()
        assert len(rows) == 0


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_migrations(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    db.setup_migrations()

    all_migrations = migrations.source_migrations(
        dir="chromadb/test/migrations", scope=db.migration_scope()
    )

    with db.tx() as cur:
        unapplied_migrations = migrations.validate_migrations(cur, all_migrations)

        assert all_migrations == unapplied_migrations

        rows = cur.execute("SELECT * FROM migrations").fetchall()
        assert len(rows) == 0

        for m in unapplied_migrations[:-1]:
            migrations.apply_migration(cur, m)

        unapplied_migrations = migrations.validate_migrations(cur, all_migrations)

        assert len(unapplied_migrations) == 1
        assert unapplied_migrations[0]["version"] == 3
        assert len(cur.execute("SELECT * FROM migrations").fetchall()) == 2
        assert len(cur.execute("SELECT * FROM table1").fetchall()) == 0
        assert len(cur.execute("SELECT * FROM table2").fetchall()) == 0
        with pytest.raises(Exception):
            cur.execute("SELECT * FROM table3").fetchall()

        for m in unapplied_migrations:
            migrations.apply_migration(cur, m)

        unapplied_migrations = migrations.validate_migrations(cur, all_migrations)

        assert len(unapplied_migrations) == 0
        assert len(cur.execute("SELECT * FROM migrations").fetchall()) == 3
        assert len(cur.execute("SELECT * FROM table3").fetchall()) == 0


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_tampered_migration(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    db.setup_migrations()

    all_migrations = migrations.source_migrations(
        dir="chromadb/test/migrations", scope=db.migration_scope()
    )

    with db.tx() as cur:
        unapplied_migrations = migrations.validate_migrations(cur, all_migrations)

        for m in unapplied_migrations:
            migrations.apply_migration(cur, m)

        migrations.validate_migrations(cur, all_migrations)

        inconsistent_version_migrations = copy.deepcopy(all_migrations)
        inconsistent_version_migrations[0]["version"] = 2

        with pytest.raises(migrations.InconsistentVersionError):
            migrations.validate_migrations(cur, inconsistent_version_migrations)

        inconsistent_hash_migrations = copy.deepcopy(all_migrations)
        inconsistent_hash_migrations[0]["hash"] = "badhash"

        with pytest.raises(migrations.InconsistentHashError):
            migrations.validate_migrations(cur, inconsistent_hash_migrations)
