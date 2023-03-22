import chromadb.db.duckdb2
import chromadb.db.migrations as migrations
from chromadb.config import Settings
import pytest
import copy


@pytest.fixture
def duckdb_db():
    return chromadb.db.duckdb2.DuckDB2(Settings(duckdb_database=":memory:"))


test_dbs = [duckdb_db]


@pytest.mark.parametrize("db_fixture", test_dbs)
def test_segment_read_write(db_fixture, request):

    db = request.getfixturevalue(db_fixture.__name__)

    with pytest.raises(Exception):
        with db.tx() as cursor:
            raise (Exception("test exception"))
