import pytest
import unittest
import os
from unittest.mock import patch

import chromadb
import chromadb.config

class GetDBTest(unittest.TestCase):

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    def test_default_db(self, mock):
        db = chromadb.get_db(chromadb.config.Settings(chroma_cache_dir="./foo"))
        assert mock.called


    @patch('chroma.db.duckdb.PersistentDuckDB', autospec=True)
    def test_persistent_duckdb(self, mock):
        db = chromadb.get_db(chromadb.config.Settings(chroma_db_impl="duckdb+parquet",
                                                  chroma_cache_dir="./foo"))
        assert mock.called


    @patch('chroma.db.clickhouse.Clickhouse', autospec=True)
    def test_clickhouse(self, mock):
        db = chromadb.get_db(chromadb.config.Settings(chroma_db_impl="clickhouse",
                                                  chroma_cache_dir="./foo",
                                                  clickhouse_host="foo",
                                                  clickhouse_port=666))
        assert mock.called

class GetAPITest(unittest.TestCase):

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    @patch('chroma.api.local.LocalAPI', autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_local(self, mock_api, mock_db):
        api = chromadb.Client(chromadb.config.Settings(chroma_cache_dir="./foo"))
        assert mock_api.called
        assert mock_db.called

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    @patch('chroma.api.celery.CeleryAPI', autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_celery(self, mock_api, mock_db):
        api = chromadb.Client(chromadb.config.Settings(chroma_api_impl="celery",
                                                    chroma_cache_dir="./foo",
                                                    celery_broker_url="foo",
                                                    celery_result_backend="foo"))
        assert mock_api.called
        assert mock_db.called


    @patch('chroma.api.fastapi.FastAPI', autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_fastapi(self, mock):
        api = chromadb.Client(chromadb.config.Settings(chroma_api_impl="rest",
                                                    chroma_cache_dir="./foo",
                                                    chroma_server_host='foo',
                                                    chroma_server_http_port='80'))
        assert mock.called


    @patch('chroma.api.arrowflight.ArrowFlightAPI', autospec=True)
    @patch.dict(os.environ, {}, clear=True)
    def test_arrowflight(self, mock):
        api = chromadb.Client(chromadb.config.Settings(chroma_api_impl="arrowflight",
                                                    chroma_cache_dir="./foo",
                                                    chroma_server_host='foo',
                                                    chroma_server_grpc_port='9999'))
        assert mock.called

