import unittest
import os
from unittest.mock import patch, Mock

import chromadb
import chromadb.config
from chromadb.db import DB


class GetDBTest(unittest.TestCase):
    @patch("chromadb.db.duckdb.DuckDB", autospec=True)
    def test_default_db(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(persist_directory="./foo")
        )
        system.instance(DB)
        assert mock.called

    @patch("chromadb.db.duckdb.PersistentDuckDB", autospec=True)
    def test_persistent_duckdb(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(
                chroma_db_impl="duckdb+parquet", persist_directory="./foo"
            )
        )
        system.instance(DB)
        assert mock.called

    @patch("chromadb.db.clickhouse.Clickhouse", autospec=True)
    def test_clickhouse(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(
                chroma_db_impl="clickhouse",
                persist_directory="./foo",
                clickhouse_host="foo",
                clickhouse_port="666",
            )
        )
        system.instance(DB)
        assert mock.called


class GetAPITest(unittest.TestCase):
    @patch("chromadb.api.local.LocalAPI", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_local(self, mock_api: Mock) -> None:
        chromadb.Client(chromadb.config.Settings(persist_directory="./foo"))
        assert mock_api.called

    @patch("chromadb.db.duckdb.DuckDB", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_local_db(self, mock_db: Mock) -> None:
        chromadb.Client(chromadb.config.Settings(persist_directory="./foo"))
        assert mock_db.called

    @patch("chromadb.api.fastapi.FastAPI", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_fastapi(self, mock: Mock) -> None:
        chromadb.Client(
            chromadb.config.Settings(
                chroma_api_impl="rest",
                persist_directory="./foo",
                chroma_server_host="foo",
                chroma_server_http_port="80",
            )
        )
        assert mock.called

    @patch("chromadb.api.fastapi.FastAPI", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_settings_pass_to_fastapi(self, mock: Mock) -> None:
        settings = chromadb.config.Settings(
            chroma_api_impl="rest",
            chroma_server_host="foo",
            chroma_server_http_port="80",
            chroma_server_headers={"foo": "bar"},
        )
        chromadb.Client(settings)

        # Check that the mock was called
        assert mock.called

        # Retrieve the arguments with which the mock was called
        # `call_args` returns a tuple, where the first element is a tuple of positional arguments
        # and the second element is a dictionary of keyword arguments. We assume here that
        # the settings object is passed as a positional argument.
        args, kwargs = mock.call_args
        passed_settings = args[0] if args else None

        # Check if the settings passed to the mock match the settings we used
        # raise Exception(passed_settings.settings)
        assert passed_settings.settings == settings
