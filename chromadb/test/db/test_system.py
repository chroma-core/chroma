import pytest
from typing import Generator, List, Callable
from chromadb.db.impl.sqlite import SqliteDB
from chromadb.config import System, Settings
from chromadb.db.system import SysDB
from pytest import FixtureRequest


def sqlite() -> Generator[SysDB, None, None]:
    """Fixture generator for sqlite DB"""
    yield SqliteDB(System(Settings(sqlite_database=":memory:", allow_reset=True)))


def db_fixtures() -> List[Callable[[], Generator[SysDB, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=db_fixtures())
def sysdb(request: FixtureRequest) -> Generator[SysDB, None, None]:
    yield next(request.param())


def test_create_delete_collections(sysdb: SysDB) -> None:
    sysdb.reset()
