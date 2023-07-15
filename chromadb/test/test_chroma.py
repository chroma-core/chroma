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
