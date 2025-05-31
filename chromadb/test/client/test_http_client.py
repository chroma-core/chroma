import json
import os
import uuid

import pytest
from pytest_httpserver import HTTPServer

import chromadb
from chromadb import DEFAULT_TENANT, DEFAULT_DATABASE
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.auth import UserIdentity
from chromadb.types import Tenant, Database


@pytest.fixture(scope="function")
def mock_server(httpserver: HTTPServer):
    identity = UserIdentity(
        user_id="", tenant=DEFAULT_TENANT, databases=[DEFAULT_DATABASE]
    )
    default_tenant = Tenant(name=DEFAULT_TENANT)
    default_database = Database(
        name=DEFAULT_DATABASE, tenant=DEFAULT_TENANT, id=uuid.uuid4()
    )
    httpserver.expect_request("/api/v2/auth/identity").respond_with_data(
        json.dumps(identity.__dict__)
    )
    httpserver.expect_request("/api/v2/tenants/default_tenant").respond_with_data(
        json.dumps(default_tenant)
    )
    httpserver.expect_request(
        "/api/v2/tenants/default_tenant/databases/default_database"
    ).respond_with_data(json.dumps({k: str(v) for k, v in default_database.items()}))
    return httpserver


def test_http_client_cardinality_with_same_settings(mock_server: HTTPServer):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    SharedSystemClient._identifier_to_system.clear()
    for _ in range(10):
        chromadb.HttpClient(host=f"http://{mock_server.host}:{mock_server.port}")
    assert len(SharedSystemClient._identifier_to_system.keys()) == 1


def test_http_client_cardinality_with_different_settings(mock_server: HTTPServer):
    if os.environ.get("CHROMA_INTEGRATION_TEST_ONLY"):
        pytest.skip("Skipping test for integration test")
    SharedSystemClient._identifier_to_system.clear()
    expected_clients_count = 10
    for i in range(expected_clients_count):
        chromadb.HttpClient(
            host=f"http://{mock_server.host}:{mock_server.port}",
            headers={"header": str(i)},
        )
    assert (
        len(SharedSystemClient._identifier_to_system.keys()) == expected_clients_count
    )
