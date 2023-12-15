import json
from unittest.mock import patch

import pytest
from pytest_httpserver import HTTPServer

import chromadb


def test_new_release_available(caplog: pytest.LogCaptureFixture) -> None:
    with patch(
        "chromadb.api.client.Client._upgrade_check_url",
        new="http://localhost:8008/pypi/chromadb/json",
    ):
        with HTTPServer(port=8008) as httpserver:
            # Define the response
            httpserver.expect_request("/pypi/chromadb/json").respond_with_data(
                json.dumps({"info": {"version": "99.99.99"}})
            )

            # Your code that makes the HTTP call
            chromadb.Client()

            assert "A new release of chromadb is available" in caplog.text


def test_on_latest_release(caplog: pytest.LogCaptureFixture) -> None:
    with HTTPServer(port=8008) as httpserver:
        # Define the response
        httpserver.expect_request("/pypi/chromadb/json").respond_with_data(
            json.dumps({"info": {"version": chromadb.__version__}})
        )

        # Your code that makes the HTTP call
        chromadb.Client()

        assert "A new release of chromadb is available" not in caplog.text


def test_local_version_newer_than_latest(caplog: pytest.LogCaptureFixture) -> None:
    with patch(
        "chromadb.api.client.Client._upgrade_check_url",
        new="http://localhost:8008/pypi/chromadb/json",
    ):
        with HTTPServer(port=8008) as httpserver:
            # Define the response
            httpserver.expect_request("/pypi/chromadb/json").respond_with_data(
                json.dumps({"info": {"version": "0.0.1"}})
            )

            # Your code that makes the HTTP call
            chromadb.Client()

            assert "A new release of chromadb is available" not in caplog.text


def test_pypi_unavailable(caplog: pytest.LogCaptureFixture) -> None:
    with patch(
        "chromadb.api.client.Client._upgrade_check_url",
        new="http://localhost:8008/pypi/chromadb/json",
    ):
        with HTTPServer(port=8009) as httpserver:
            # Define the response
            httpserver.expect_request("/pypi/chromadb/json").respond_with_data(
                json.dumps({"info": {"version": "99.99.99"}})
            )

            chromadb.Client()

            assert "A new release of chromadb is available" not in caplog.text
