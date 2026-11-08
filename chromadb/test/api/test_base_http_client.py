import httpx2
import pytest

import chromadb.errors as errors
from chromadb.api.base_http_client import BaseHTTPClient


def _error_response(body: object) -> httpx2.Response:
    return httpx2.Response(
        status_code=400,
        json=body,
        request=httpx2.Request("GET", "http://localhost/api/v2/test"),
    )


def test_raise_chroma_error_uses_response_message() -> None:
    response = _error_response(
        {"error": "InvalidArgumentError", "message": "expected failure"}
    )

    with pytest.raises(errors.InvalidArgumentError, match="expected failure"):
        BaseHTTPClient._raise_chroma_error(response)


def test_raise_chroma_error_requires_response_message() -> None:
    response = _error_response({"error": "InvalidArgumentError"})

    with pytest.raises(ValueError, match="missing required 'message' field") as error:
        BaseHTTPClient._raise_chroma_error(response)

    assert isinstance(error.value.__cause__, KeyError)


def test_raise_chroma_error_maps_conditional_write_conflict() -> None:
    request = httpx2.Request("POST", "http://localhost/conditional/commit")
    response = httpx2.Response(
        409,
        request=request,
        json={
            "error": "ConditionalWriteConflictError",
            "message": "conditional write conflict",
        },
    )

    with pytest.raises(
        errors.ConditionalWriteConflictError, match="conditional write conflict"
    ):
        BaseHTTPClient._raise_chroma_error(response)


def test_raise_chroma_error_maps_generic_conditional_write_conflict() -> None:
    request = httpx2.Request("POST", "http://localhost/conditional/commit")
    response = httpx2.Response(
        409,
        request=request,
        json={
            "error": "ChromaError",
            "message": "conditional write conflict",
        },
    )

    with pytest.raises(
        errors.ConditionalWriteConflictError, match="conditional write conflict"
    ):
        BaseHTTPClient._raise_chroma_error(response)


def test_raise_chroma_error_maps_transactions_not_supported() -> None:
    request = httpx2.Request("POST", "http://localhost/conditional/commit")
    response = httpx2.Response(
        501,
        request=request,
        json={
            "error": "TransactionsNotSupported",
            "message": "conditional transactions require the gRPC log implementation",
        },
    )

    with pytest.raises(
        errors.TransactionsNotSupportedError,
        match="conditional transactions require the gRPC log implementation",
    ):
        BaseHTTPClient._raise_chroma_error(response)
