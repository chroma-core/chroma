"""Regression tests for Chroma issue #7953:
Validate that negative or >u32 values for limit, offset, and n_results
are rejected at request model validation with a ValidationError (HTTP 400),
preventing unhandled 500 OverflowError panics from PyO3 Rust bindings.
"""

import pytest
from pydantic import ValidationError

from chromadb.server.fastapi.types import (
    MAX_U32,
    DeleteEmbedding,
    GetEmbedding,
    QueryEmbedding,
)


@pytest.mark.parametrize(
    "invalid_val",
    [-1, -100, MAX_U32 + 1, 10**18],
)
def test_get_embedding_rejects_out_of_bounds_limit(invalid_val: int) -> None:
    with pytest.raises(ValidationError):
        GetEmbedding(limit=invalid_val)


@pytest.mark.parametrize(
    "invalid_val",
    [-1, -100, MAX_U32 + 1, 10**18],
)
def test_get_embedding_rejects_out_of_bounds_offset(invalid_val: int) -> None:
    with pytest.raises(ValidationError):
        GetEmbedding(offset=invalid_val)


@pytest.mark.parametrize(
    "invalid_val",
    [-1, -5, MAX_U32 + 1, 10**18],
)
def test_query_embedding_rejects_out_of_bounds_n_results(invalid_val: int) -> None:
    with pytest.raises(ValidationError):
        QueryEmbedding(query_embeddings=[[0.1, 0.2]], n_results=invalid_val)


@pytest.mark.parametrize(
    "invalid_val",
    [-1, -5, MAX_U32 + 1, 10**18],
)
def test_delete_embedding_rejects_out_of_bounds_limit(invalid_val: int) -> None:
    with pytest.raises(ValidationError):
        DeleteEmbedding(limit=invalid_val)


@pytest.mark.parametrize("valid_val", [0, 1, 100, MAX_U32])
def test_get_embedding_accepts_valid_bounds(valid_val: int) -> None:
    req = GetEmbedding(limit=valid_val, offset=valid_val)
    assert req.limit == valid_val
    assert req.offset == valid_val


@pytest.mark.parametrize("valid_val", [0, 1, 10, MAX_U32])
def test_query_embedding_accepts_valid_bounds(valid_val: int) -> None:
    req = QueryEmbedding(query_embeddings=[[0.1, 0.2]], n_results=valid_val)
    assert req.n_results == valid_val


@pytest.mark.parametrize("valid_val", [0, 1, 50, MAX_U32])
def test_delete_embedding_accepts_valid_bounds(valid_val: int) -> None:
    req = DeleteEmbedding(limit=valid_val)
    assert req.limit == valid_val
