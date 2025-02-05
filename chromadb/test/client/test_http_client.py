import json
import uuid

import pytest
from pytest_httpserver import HTTPServer

import chromadb
from chromadb import DEFAULT_TENANT, DEFAULT_DATABASE
from chromadb.auth import UserIdentity
from chromadb.types import Tenant, Database


@pytest.fixture
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
    client: chromadb.HttpClient = None
    for _ in range(10):
        client = chromadb.HttpClient(host=mock_server.host, port=mock_server.port)
    assert len(client._identifier_to_system.keys()) == 1


def test_http_client_cardinality_with_different_settings(mock_server: HTTPServer):
    client: chromadb.HttpClient = None
    for i in range(10):
        client = chromadb.HttpClient(
            host=mock_server.host, port=mock_server.port, headers={"header": str(i)}
        )
    assert len(client._identifier_to_system.keys()) == 10
