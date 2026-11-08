"""Regression tests for Knn key handling in _embed_knn_string_queries."""

from typing import Any, List

import pytest

from chromadb.api.models.CollectionCommon import CollectionCommon
from chromadb.execution.expression.operator import Key, Knn


class _StubCollection:
    """Minimal stand-in exposing only what _embed_knn_string_queries touches."""

    schema = None

    def __init__(self) -> None:
        self.embed_calls: List[Any] = []

    def _embed(self, input: Any, is_query: bool = False) -> Any:
        self.embed_calls.append(input)
        return [[0.0, 1.0, 2.0]]


def _embed_knn(stub: _StubCollection, knn: Knn) -> Any:
    return CollectionCommon._embed_knn_string_queries(stub, knn)  # type: ignore[arg-type]


def test_knn_key_object_routes_like_its_string() -> None:
    """A Key for a non-embedding field must not fall through to the default
    embedding function. Key.__eq__ returns an Eq expression, which is always
    truthy, so an unnormalized comparison sent every Key down that branch."""
    as_string = _StubCollection()
    with pytest.raises(ValueError, match="key not found in schema"):
        _embed_knn(as_string, Knn(query="hello", key="not_a_real_field"))

    as_key = _StubCollection()
    with pytest.raises(ValueError, match="key not found in schema"):
        _embed_knn(as_key, Knn(query="hello", key=Key("not_a_real_field")))

    assert as_key.embed_calls == [], "a non-embedding key must not use the default embedding function"


def test_knn_embedding_key_object_still_embeds() -> None:
    """The Key spelling of the main embedding field keeps working."""
    stub = _StubCollection()
    result = _embed_knn(stub, Knn(query="hello", key=Key("#embedding")))

    assert stub.embed_calls == [["hello"]], "main embedding field should use the default function"
    assert result.query == [0.0, 1.0, 2.0]
