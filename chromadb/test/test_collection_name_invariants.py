import re
import pytest

def validate_collection_name(name: str) -> bool:
    if not (3 <= len(name) <= 63):
        return False
    if not re.match(r"^[a-zA-Z0-9][a-zA-Z0-9._-]*[a-zA-Z0-9]$", name):
        return False
    if ".." in name:
        return False
    return True

def test_valid_collection_names():
    assert validate_collection_name("valid_name_123") is True
    assert validate_collection_name("my-docs-collection") is True

def test_invalid_collection_names():
    assert validate_collection_name("ab") is False
    assert validate_collection_name("invalid..name") is False
    assert validate_collection_name("-starts-with-dash") is False
