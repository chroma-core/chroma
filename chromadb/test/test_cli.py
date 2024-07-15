from typer.testing import CliRunner

from chromadb.cli.cli import app
from chromadb.cli.utils import set_log_file_path
from chromadb.config import System
from chromadb.db.base import get_sql
from chromadb.db.impl.sqlite import SqliteDB
from pypika import Table

runner = CliRunner()


def test_app() -> None:
    result = runner.invoke(
        app,
        [
            "run",
            "--path",
            "chroma_test_data",
            "--port",
            "8001",
            "--test",
        ],
    )
    assert "chroma_test_data" in result.stdout
    assert "8001" in result.stdout


def test_utils_set_log_file_path() -> None:
    log_config = set_log_file_path("chromadb/log_config.yml", "test.log")
    assert log_config["handlers"]["file"]["filename"] == "test.log"


def test_vacuum(sqlite_persistent: System) -> None:
    system = sqlite_persistent
    sqlite = system.instance(SqliteDB)

    # Maintenance log should be empty
    with sqlite.tx() as cur:
        t = Table("maintenance_log")
        q = sqlite.querybuilder().from_(t).select("*")
        sql, params = get_sql(q)
        cur.execute(sql, params)
        assert cur.fetchall() == []

    result = runner.invoke(app, ["vacuum", "--path", system.settings.persist_directory])
    print(result.stdout)
    assert result.exit_code == 0

    # Maintenance log should have a vacuum entry
    with sqlite.tx() as cur:
        t = Table("maintenance_log")
        q = sqlite.querybuilder().from_(t).select("*")
        sql, params = get_sql(q)
        cur.execute(sql, params)
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][2] == "vacuum"
