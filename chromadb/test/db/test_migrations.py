import pytest
from typing import Generator, List, Callable
from chromadb.db.migrations import MigratableDB
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings
from pytest import FixtureRequest


def sqlite() -> Generator[MigratableDB, None, None]:
    """Fixture generator for sqlite DB"""
    yield SqliteDB(System(Settings(sqlite_database=":memory:", migrations="none")))


def db_fixtures() -> List[Callable[[], Generator[MigratableDB, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=db_fixtures())
def db(request: FixtureRequest) -> Generator[MigratableDB, None, None]:
    yield next(request.param())


# Some Database impls improperly swallow exceptions, test that the wrapper works
def test_exception_propagation(db: MigratableDB) -> None:
    with pytest.raises(Exception):
        with db.tx():
            raise (Exception("test exception"))


def test_setup_migrations(db: MigratableDB) -> None:
    db.reset()
    db.setup_migrations()
    db.setup_migrations()  # idempotent

    with db.tx() as cursor:
        rows = cursor.execute("SELECT * FROM migrations").fetchall()
        assert len(rows) == 0
