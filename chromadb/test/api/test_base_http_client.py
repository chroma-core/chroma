import httpx
import pytest

from chromadb.api.base_http_client import BaseHTTPClient
from chromadb.errors import ConditionalWriteConflictError


def test_raise_chroma_error_maps_conditional_write_conflict() -> None:
    request = httpx.Request("POST", "http://localhost/conditional/commit")
    response = httpx.Response(
        409,
        request=request,
        json={
            "error": "ConditionalWriteConflictError",
            "message": "conditional write conflict",
        },
    )

    with pytest.raises(ConditionalWriteConflictError, match="conditional write conflict"):
        BaseHTTPClient._raise_chroma_error(response)
