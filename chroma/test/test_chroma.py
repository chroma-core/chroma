import pytest
import unittest
from unittest.mock import patch

import chroma
import chroma.config

class GetDBTest(unittest.TestCase):

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    def test_default_db(self, mock):
        db = chroma.get_db(chroma.config.Settings(chroma_cache_dir="./foo"))
        assert mock.called


    @patch('chroma.db.duckdb.PersistentDuckDB', autospec=True)
    def test_persistent_duckdb(self, mock):
        db = chroma.get_db(chroma.config.Settings(chroma_db_impl="duckdb+parquet",
                                                  chroma_cache_dir="./foo"))
        assert mock.called


    @patch('chroma.db.clickhouse.Clickhouse', autospec=True)
    def test_clickhouse(self, mock):
        db = chroma.get_db(chroma.config.Settings(chroma_db_impl="clickhouse",
                                                  chroma_cache_dir="./foo",
                                                  clickhouse_host="foo",
                                                  clickhouse_port=666))
        assert mock.called

class GetAPITest(unittest.TestCase):

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    @patch('chroma.api.local.LocalAPI', autospec=True)
    def test_local(self, mock_api, mock_db):
        api = chroma.get_api(chroma.config.Settings(chroma_cache_dir="./foo"))
        assert mock_api.called
        assert mock_db.called

    @patch('chroma.db.duckdb.DuckDB', autospec=True)
    @patch('chroma.api.celery.CeleryAPI', autospec=True)
    def test_celery(self, mock_api, mock_db):
        api = chroma.get_api(chroma.config.Settings(chroma_api_impl="celery",
                                                    chroma_cache_dir="./foo",
                                                    celery_broker_url="foo",
                                                    celery_result_backend="foo"))
        assert mock_api.called
        assert mock_db.called


    @patch('chroma.api.fastapi.FastAPI', autospec=True)
    def test_fastapi(self, mock):
        api = chroma.get_api(chroma.config.Settings(chroma_api_impl="rest",
                                                    chroma_cache_dir="./foo",
                                                    chroma_server_host='foo',
                                                    chroma_server_http_port='80'))
        assert mock.called


    @patch('chroma.api.arrowflight.ArrowFlightAPI', autospec=True)
    def test_arrowflight(self, mock):
        api = chroma.get_api(chroma.config.Settings(chroma_api_impl="arrowflight",
                                                    chroma_cache_dir="./foo",
                                                    chroma_server_host='foo',
                                                    chroma_server_grpc_port='9999'))
        assert mock.called

