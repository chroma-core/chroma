"""Regression tests for `ids` on the v2 query endpoint.

`QueryEmbedding` had no `ids` field, so pydantic discarded it from the request body and
the server called `_query` without it. The HTTP and Cloud clients both send `ids`, and the
in-process API honours it, so the filter silently did nothing for every server-backed user
while returning a completely normal-looking result.
"""

import os
import tempfile
from typing import Any, Dict, List

import pytest

from chromadb.config import Settings
from chromadb.server.fastapi import FastAPI
from chromadb.server.fastapi.types import GetEmbedding, QueryEmbedding

os.environ.setdefault("ANONYMIZED_TELEMETRY", "False")


def test_query_embedding_keeps_ids() -> None:
    """The request model has to carry the field at all, or pydantic drops it."""
    q = QueryEmbedding(query_embeddings=[[1.0, 0.0]], ids=["c"])  # type: ignore[arg-type]

    assert q.ids == ["c"]


def test_query_embedding_ids_defaults_to_none() -> None:
    assert QueryEmbedding(query_embeddings=[[1.0, 0.0]]).ids is None  # type: ignore[arg-type]


def test_query_embedding_matches_get_embedding_on_ids() -> None:
    """The sibling model in the same file has always declared `ids`; keep them aligned."""
    assert "ids" in QueryEmbedding.model_fields
    assert "ids" in GetEmbedding.model_fields
    assert (
        QueryEmbedding.model_fields["ids"].annotation
        == GetEmbedding.model_fields["ids"].annotation
    )


@pytest.fixture
def server_client():
    from starlette.testclient import TestClient

    with tempfile.TemporaryDirectory() as persist_directory:
        settings = Settings(
            anonymized_telemetry=False,
            is_persistent=True,
            persist_directory=persist_directory,
        )
        yield TestClient(FastAPI(settings).app())


def _seed(client: Any) -> str:
    base = "/api/v2/tenants/default_tenant/databases/default_database/collections"
    created = client.post(
        base,
        json={
            "name": "query_ids",
            "metadata": {"hnsw:space": "cosine"},
            "get_or_create": True,
        },
    )
    assert created.status_code == 200, created.text
    collection_id = created.json()["id"]

    added = client.post(
        f"{base}/{collection_id}/add",
        json={
            "ids": ["a", "b", "c"],
            "embeddings": [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]],
            "documents": ["A", "B", "C"],
            "metadatas": [{"keep": "yes"}, {"keep": "no"}, {"keep": "yes"}],
        },
    )
    assert added.status_code == 201, added.text
    return f"{base}/{collection_id}"


def test_query_honours_ids(server_client) -> None:
    path = _seed(server_client)
    query = {"query_embeddings": [[1.0, 0.0]], "n_results": 3, "include": []}

    filtered = server_client.post(f"{path}/query", json={**query, "ids": ["c"]})
    assert filtered.status_code == 200, filtered.text

    assert filtered.json()["ids"] == [["c"]]


def test_query_without_ids_is_unaffected(server_client) -> None:
    """The control: no `ids` still searches the whole collection."""
    path = _seed(server_client)

    unfiltered = server_client.post(
        f"{path}/query",
        json={"query_embeddings": [[1.0, 0.0]], "n_results": 3, "include": []},
    )
    assert unfiltered.status_code == 200, unfiltered.text

    assert sorted(unfiltered.json()["ids"][0]) == ["a", "b", "c"]


def test_query_ids_narrows_a_subset(server_client) -> None:
    """More than one id: every returned row must be one of them, and all of them present."""
    path = _seed(server_client)

    response = server_client.post(
        f"{path}/query",
        json={
            "query_embeddings": [[1.0, 0.0]],
            "ids": ["c", "b"],
            "n_results": 3,
            "include": [],
        },
    )
    assert response.status_code == 200, response.text

    returned: List[str] = response.json()["ids"][0]
    assert sorted(returned) == ["b", "c"]


def test_get_still_honours_ids(server_client) -> None:
    """The sibling endpoint that already worked; a control on the change."""
    path = _seed(server_client)

    response = server_client.post(f"{path}/get", json={"ids": ["c"]})
    assert response.status_code == 200, response.text

    assert response.json()["ids"] == ["c"]


def test_ids_combines_with_where(server_client) -> None:
    """`ids` and `where` are separate filter inputs; both should still apply."""
    path = _seed(server_client)
    base = "/api/v2/tenants/default_tenant/databases/default_database/collections"

    added = server_client.post(
        f"{base}/{path.rsplit('/', 1)[1]}/add",
        json={
            "ids": ["d"],
            "embeddings": [[0.95, 0.05]],
            "documents": ["D"],
            "metadatas": [{"keep": "no"}],
        },
    )
    assert added.status_code == 201, added.text

    response = server_client.post(
        f"{path}/query",
        json={
            "query_embeddings": [[1.0, 0.0]],
            "ids": ["c", "d"],
            "where": {"keep": "yes"},
            "n_results": 3,
            "include": [],
        },
    )
    assert response.status_code == 200, response.text

    assert response.json()["ids"] == [["c"]]


def test_openapi_schema_advertises_ids(server_client) -> None:
    """The field has to reach the schema, since the client builds requests from it."""
    schema: Dict[str, Any] = server_client.get("/openapi.json").json()

    # The v2 routes inline the request model instead of naming it, so read the schema
    # off the query operation itself.
    query = schema["paths"][
        "/api/v2/tenants/{tenant}/databases/{database_name}"
        "/collections/{collection_id}/query"
    ]["post"]
    properties = query["requestBody"]["content"]["application/json"]["schema"][
        "properties"
    ]

    assert "query_embeddings" in properties
    assert "ids" in properties
