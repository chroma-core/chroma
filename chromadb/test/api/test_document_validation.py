import pytest

from chromadb.api.types import validate_documents


def test_validate_documents_accepts_plain_strings() -> None:
    validate_documents(["hello", "world"])


def test_validate_documents_rejects_non_string() -> None:
    with pytest.raises(ValueError, match="Expected document to be a str"):
        validate_documents(["ok", 42])  # type: ignore[list-item]


def test_validate_documents_rejects_nul_bytes() -> None:
    # Embedded NUL bytes corrupt the SQLite FTS5 inverted index for the whole
    # collection (see #7388), so they must be rejected up front.
    with pytest.raises(ValueError, match="NUL"):
        validate_documents(["before\x00after"])


def test_validate_documents_nullable_allows_none() -> None:
    validate_documents(["ok", None], nullable=True)  # type: ignore[list-item]
