from typer.testing import CliRunner

from chromadb.cli.cli import app
from chromadb.cli.utils import set_log_file_path

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
