import json
import uuid
import time

import pytest
from hypothesis import given, strategies as st
from pytest_httpserver import HTTPServer

import chromadb
from chromadb.api.client import SharedSystemClient
from chromadb.errors import GenericError
from chromadb.types import Tenant, Database


@pytest.fixture(autouse=True)
def reset_client_settings() -> None:
    SharedSystemClient.clear_system_cache()


def test_incompatible_server_version(caplog: pytest.LogCaptureFixture) -> None:
    with HTTPServer(port=8001) as httpserver:
        httpserver.expect_request("/api/v1/collections").respond_with_data(
            json.dumps([])
        )
        httpserver.expect_request("/api/v1/heartbeat").respond_with_data(
            json.dumps({"nanosecond heartbeat": int(time.time_ns())})
        )

        httpserver.expect_request("/api/v1").respond_with_data(
            json.dumps({"nanosecond heartbeat": int(time.time_ns())})
        )

        httpserver.expect_request("/api/v1/version").respond_with_data(
            json.dumps("0.4.1")
        )
        client = chromadb.HttpClient("http://localhost:8001")

        with pytest.raises(ValueError) as e:
            client.list_collections()
        assert "It appears you are using newer version of Chroma client" in str(e.value)


def test_compatible_server_version(caplog: pytest.LogCaptureFixture) -> None:
    with HTTPServer(port=8001) as httpserver:
        httpserver.expect_request("/api/v1/collections").respond_with_data(
            json.dumps([])
        )
        httpserver.expect_request("/api/v1/heartbeat").respond_with_data(
            json.dumps({"nanosecond heartbeat": int(time.time_ns())})
        )

        httpserver.expect_request("/api/v1").respond_with_data(
            json.dumps({"nanosecond heartbeat": int(time.time_ns())})
        )

        httpserver.expect_request("/api/v1/version").respond_with_data(
            json.dumps("0.4.15")
        )
        httpserver.expect_request("/api/v1/tenants/default_tenant").respond_with_data(
            json.dumps(Tenant(name="default_tenant"))
        )
        httpserver.expect_request(
            "/api/v1/databases/default_database"
        ).respond_with_data(
            json.dumps(
                Database(
                    name="default_database",
                    tenant="default_tenant",
                    id=str(uuid.uuid4()),  # type: ignore
                )
            )
        )

        client = chromadb.HttpClient("http://localhost:8001")

        client.list_collections()


def test_client_server_not_available(caplog: pytest.LogCaptureFixture) -> None:
    with HTTPServer(port=8002) as _:
        client = chromadb.HttpClient("http://localhost:8001")

        with pytest.raises(GenericError) as e:
            client.list_collections()
        assert "Chroma server seems inaccessible" in str(e.value)


@given(status=st.sampled_from([502, 503, 504]))
def test_client_server_with_proxy_error(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    with HTTPServer(port=8001) as httpserver:
        httpserver.expect_request("/api/v1/heartbeat").respond_with_data(
            "Oh no!", status=status
        )

        httpserver.expect_request("/api/v1").respond_with_data("Oh no!", status=status)
        client = chromadb.HttpClient("http://localhost:8001")

        with pytest.raises(GenericError) as e:
            client.list_collections()
        print(str(e.value))
        assert "Your proxy reports Chroma server might not be" in str(e.value)
