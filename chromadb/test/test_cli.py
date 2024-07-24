import multiprocessing
import multiprocessing.context
from multiprocessing.synchronize import Event

from typer.testing import CliRunner

from chromadb.cli.cli import app
from chromadb.cli.utils import set_log_file_path
from chromadb.config import Settings, System
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

    # This is True because it's a fresh system, so let's set it to False to test that the vacuum command enables it
    config = sqlite.config
    config.set_parameter("automatically_prune", False)
    sqlite.set_config(config)

    # Maintenance log should be empty
    with sqlite.tx() as cur:
        t = Table("maintenance_log")
        q = sqlite.querybuilder().from_(t).select("*")
        sql, params = get_sql(q)
        cur.execute(sql, params)
        assert cur.fetchall() == []

    result = runner.invoke(
        app,
        ["utils", "vacuum", "--path", system.settings.persist_directory],
        input="y\n",
    )
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

    # Automatic pruning should have been enabled
    assert sqlite.config.get_parameter("automatically_prune").value


def simulate_transactional_write(
    settings: Settings, ready_event: Event, shutdown_event: Event
) -> None:
    system = System(settings=settings)
    system.start()
    sqlite = system.instance(SqliteDB)

    with sqlite.tx() as cur:
        cur.execute("INSERT INTO tenants DEFAULT VALUES")
        ready_event.set()
        shutdown_event.wait()

    system.stop()


def test_vacuum_errors_if_locked(sqlite_persistent: System) -> None:
    """Vacuum command should fail with details if there is a long-lived lock on the database."""
    ctx = multiprocessing.get_context("spawn")
    ready_event = ctx.Event()
    shutdown_event = ctx.Event()
    process = ctx.Process(
        target=simulate_transactional_write,
        args=(sqlite_persistent.settings, ready_event, shutdown_event),
    )
    process.start()
    ready_event.wait()

    try:
        result = runner.invoke(
            app,
            [
                "utils",
                "vacuum",
                "--path",
                sqlite_persistent.settings.persist_directory,
                "--force",
            ],
        )
        assert result.exit_code == 1
        assert "database is locked" in result.stdout
    finally:
        shutdown_event.set()
        process.join()
