"""Client-side where filters use the same recursion cap as the Rust parser."""

import pytest

from chromadb.api.types import validate_where, validate_where_document

MAX_DEPTH = 64


def _nested_where(depth: int) -> dict:
    current: dict = {"k": "v"}
    for _ in range(depth):
        current = {"$and": [current, {"k": "v"}]}
    return current


def _nested_where_document(depth: int) -> dict:
    current: dict = {"$contains": "x"}
    for _ in range(depth):
        current = {"$and": [current, {"$contains": "x"}]}
    return current


def test_where_accepts_max_depth() -> None:
    validate_where(_nested_where(MAX_DEPTH))


def test_where_rejects_depth_above_max() -> None:
    with pytest.raises(ValueError, match="nesting depth exceeds 64"):
        validate_where(_nested_where(MAX_DEPTH + 1))


def test_where_document_accepts_max_depth() -> None:
    validate_where_document(_nested_where_document(MAX_DEPTH))


def test_where_document_rejects_depth_above_max() -> None:
    with pytest.raises(ValueError, match="nesting depth exceeds 64"):
        validate_where_document(_nested_where_document(MAX_DEPTH + 1))
