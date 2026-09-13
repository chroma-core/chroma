"""Tests for collection name validation, including whitespace handling.

Regression tests for https://github.com/chroma-core/chroma/issues/7609
"""
from chromadb.api.segment import check_index_name
import pytest


def test_check_index_name_strips_leading_whitespace() -> None:
    """Leading whitespace should be stripped before validation."""
    # Should not raise - " testDB" strips to "testDB" which is valid
    check_index_name(" testDB")


def test_check_index_name_strips_trailing_whitespace() -> None:
    """Trailing whitespace should be stripped before validation."""
    # Should not raise - "testDB " strips to "testDB" which is valid
    check_index_name("testDB ")


def test_check_index_name_strips_both_whitespace() -> None:
    """Both leading and trailing whitespace should be stripped."""
    # Should not raise - " testDB " strips to "testDB" which is valid
    check_index_name(" testDB ")


def test_check_index_name_strips_tabs() -> None:
    """Tab characters should also be stripped."""
    check_index_name("\ttestDB\t")


def test_check_index_name_strips_newlines() -> None:
    """Newline characters should also be stripped."""
    check_index_name("\ntestDB\n")


def test_check_index_name_rejects_whitespace_inside() -> None:
    """Whitespace inside the name should still be rejected."""
    with pytest.raises(ValueError):
        check_index_name("test DB")


def test_check_index_name_rejects_empty_after_strip() -> None:
    """Names that are empty after stripping should be rejected."""
    with pytest.raises(ValueError):
        check_index_name("   ")


def test_check_index_name_rejects_too_short_after_strip() -> None:
    """Names shorter than 3 chars after stripping should be rejected."""
    with pytest.raises(ValueError):
        check_index_name(" ab ")


def test_check_index_name_valid_names() -> None:
    """Valid names should pass validation."""
    check_index_name("test-db")
    check_index_name("test_db")
    check_index_name("test.db")
    check_index_name("TestDB123")


def test_check_index_name_rejects_invalid_names() -> None:
    """Invalid names should still be rejected."""
    with pytest.raises(ValueError):
        check_index_name("ab")  # too short
    with pytest.raises(ValueError):
        check_index_name("a" * 64)  # too long
    with pytest.raises(ValueError):
        check_index_name(".test.")  # doesn't start/end with alphanumeric
    with pytest.raises(ValueError):
        check_index_name("test..db")  # consecutive periods
    with pytest.raises(ValueError):
        check_index_name("192.168.1.1")  # IPv4 address
