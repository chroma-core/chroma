import pytest
from typing import Generator, List, Callable
from chromadb.config import System, Settings

# from chromadb.ingest import Producer
# from chromadb.segment import MetadataReader
from pytest import FixtureRequest


def sqlite() -> Generator[System, None, None]:
    """Fixture generator for sqlite DB"""
    settings = Settings(sqlite_database=":memory:", allow_reset=True)
    system = System(settings)

    yield system

    # db = SqliteDB(System(Settings(sqlite_database=":memory:", allow_reset=True)))
    # db.start()
    # yield db
    # db.stop()


def system_fixtures() -> List[Callable[[], Generator[System, None, None]]]:
    return [sqlite]


@pytest.fixture(scope="module", params=system_fixtures())
def system(request: FixtureRequest) -> Generator[System, None, None]:
    yield next(request.param())


# insert via Producer
# check via Metadata Index
