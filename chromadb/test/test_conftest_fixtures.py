"""Tests for conftest.py fixture registration.

Regression tests for https://github.com/chroma-core/chroma/issues/7395
"""
import pytest
from chromadb.test.conftest import filtered_fixture_names


def test_filtered_fixture_names_contains_valid_fixtures() -> None:
    """Default fixture list should contain valid pytest fixture names."""
    fixtures = filtered_fixture_names()

    # These should all be registered pytest fixtures
    expected_fixtures = [
        "fastapi",
        "async_fastapi",
        "fastapi_persistent",
        "sqlite",
        "sqlite_persistent",
    ]

    for name in expected_fixtures:
        assert name in fixtures, f"Expected fixture '{name}' in default list"


def test_filtered_fixture_names_no_invalid_references() -> None:
    """Default fixture list should not reference non-existent fixtures."""
    fixtures = filtered_fixture_names()

    # These should NOT be in the list (they're not valid fixture names)
    invalid_names = [
        "sqlite_fixture",  # Should be "sqlite"
    ]

    for name in invalid_names:
        assert name not in fixtures, f"Invalid fixture reference '{name}' should not be in list"
