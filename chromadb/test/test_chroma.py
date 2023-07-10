import unittest
import os
from unittest.mock import patch, Mock
import pytest
import chromadb
import chromadb.config
from chromadb.db import DB


class GetDBTest(unittest.TestCase):
    @patch("chromadb.db.impl.sqlite.SqliteDB", autospec=True)
    def test_default_db(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(persist_directory="./foo")
        )
        system.instance(DB)
        assert mock.called

    @patch("chromadb.db.impl.sqlite.SqliteDB", autospec=True)
    def test_sqlite_sysdb(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(
                chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
                persist_directory="./foo",
            )
        )
        system.instance(DB)
        assert mock.called

    @patch("chromadb.db.impl.sqlite.SqliteDB", autospec=True)
    def test_sqlite_queue(self, mock: Mock) -> None:
        system = chromadb.config.System(
            chromadb.config.Settings(
                chroma_sysdb_impl="chromadb.db.impl.sqlite.SqliteDB",
                chroma_producer_impl="chromadb.db.impl.sqlite.SqliteDB",
                chroma_consumer_impl="chromadb.db.impl.sqlite.SqliteDB",
                persist_directory="./foo",
            )
        )
        system.instance(DB)
        assert mock.called


class GetAPITest(unittest.TestCase):
    @patch("chromadb.api.local.SegmentAPI", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_local(self, mock_api: Mock) -> None:
        chromadb.Client(chromadb.config.Settings(persist_directory="./foo"))
        assert mock_api.called

    @patch("chromadb.db.impl.sqlite.SqliteDB", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_local_db(self, mock_db: Mock) -> None:
        chromadb.Client(chromadb.config.Settings(persist_directory="./foo"))
        assert mock_db.called

    @patch("chromadb.api.fastapi.FastAPI", autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_fastapi(self, mock: Mock) -> None:
        chromadb.Client(
            chromadb.config.Settings(
                chroma_api_impl="chromadb.api.fastapi.FastAPI",
                persist_directory="./foo",
                chroma_server_host="foo",
                chroma_server_http_port="80",
            )
        )
        assert mock.called


def test_legacy_values() -> None:
    with pytest.raises(ValueError):
        chromadb.Client(
            chromadb.config.Settings(
                chroma_api_impl="chromadb.api.local.LocalAPI",
                persist_directory="./foo",
                chroma_server_host="foo",
                chroma_server_http_port="80",
            )
        )
