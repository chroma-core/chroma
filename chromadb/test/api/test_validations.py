import pytest
from chromadb.api.types import RecordSet, does_record_set_contain_data


valid_record_set: RecordSet = {
    "ids": ["1", "2", "3"],
    "embeddings": None,
    "metadatas": None,
    "documents": ["doc1", "doc2", "doc3"],
    "images": None,
    "uris": None,
}


def test_does_record_set_contain_data() -> None:
    # Test case 1: Empty-list field
    record_set_non_list: RecordSet = {
        "ids": ["1", "2", "3"],
        "embeddings": [],
        "metadatas": None,
        "documents": None,
        "images": None,
        "uris": None,
    }

    with pytest.raises(ValueError) as e:
        does_record_set_contain_data(record_set_non_list, include=["embeddings"])

    assert "Expected embeddings to be a non-empty list" in str(e)

    # Test case 2: Non-list field
    with pytest.raises(ValueError) as e:
        does_record_set_contain_data(valid_record_set, include=[])

    assert "Expected include to be a non-empty list" in str(e)

    # Test case 3: Non-existent field
    with pytest.raises(ValueError) as e:
        does_record_set_contain_data(valid_record_set, include=["non_existent_field"])

    assert (
        "Expected include key to be a a known field of RecordSet, got non_existent_field"
        in str(e)
    )
