import pytest
from pydantic import ValidationError

from chromadb.server.fastapi.types import (
    U32_MAX,
    DeleteEmbedding,
    GetEmbedding,
    QueryEmbedding,
)

OUT_OF_RANGE = [-1, -100, U32_MAX + 1, 999999999999]


@pytest.mark.parametrize("field", ["limit", "offset"])
@pytest.mark.parametrize("value", OUT_OF_RANGE)
def test_get_embedding_out_of_range_rejected(field: str, value: int) -> None:
    # Regression test: unbounded limit/offset reached the Rust bindings
    # (u32) and surfaced as an unhandled 500 OverflowError over HTTP.
    with pytest.raises(ValidationError):
        GetEmbedding(**{field: value})


@pytest.mark.parametrize("field", ["limit", "offset"])
@pytest.mark.parametrize("value", [None, 0, 1, U32_MAX])
def test_get_embedding_in_range_accepted(field: str, value: int) -> None:
    model = GetEmbedding(**{field: value})
    assert getattr(model, field) == value


@pytest.mark.parametrize("value", OUT_OF_RANGE)
def test_query_embedding_out_of_range_n_results_rejected(value: int) -> None:
    with pytest.raises(ValidationError):
        QueryEmbedding(query_embeddings=[[0.1, 0.2]], n_results=value)


@pytest.mark.parametrize("value", [0, 1, 10, U32_MAX])
def test_query_embedding_in_range_n_results_accepted(value: int) -> None:
    model = QueryEmbedding(query_embeddings=[[0.1, 0.2]], n_results=value)
    assert model.n_results == value


def test_query_embedding_default_n_results() -> None:
    model = QueryEmbedding(query_embeddings=[[0.1, 0.2]])
    assert model.n_results == 10


@pytest.mark.parametrize("value", [-1, U32_MAX + 1])
def test_delete_embedding_out_of_range_limit_rejected(value: int) -> None:
    with pytest.raises(ValidationError):
        DeleteEmbedding(limit=value)


@pytest.mark.parametrize("value", [None, 0, U32_MAX])
def test_delete_embedding_in_range_limit_accepted(value: int) -> None:
    assert DeleteEmbedding(limit=value).limit == value
