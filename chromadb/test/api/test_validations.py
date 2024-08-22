import pytest
from typing import cast

import chromadb.errors as errors

from chromadb.api.types import IDs, validate_ids


def test_ids_validation():
    ids = ["id1", "id2", "id3"]
    assert validate_ids(ids) == ids

    with pytest.raises(ValueError, match="Expected IDs to be a list"):
        validate_ids(cast(IDs, "not a list"))

    with pytest.raises(ValueError, match="Expected IDs to be a non-empty list"):
        validate_ids([])

    with pytest.raises(ValueError, match="Expected ID to be a str"):
        validate_ids(cast(IDs, ["id1", 123, "id3"]))

    with pytest.raises(ValueError, match="Expected ID to be a non-empty str"):
        validate_ids(["id1", "", "id3"])

    with pytest.raises(errors.DuplicateIDError, match="Expected IDs to be unique"):
        validate_ids(["id1", "id2", "id1"])

    ids = [
        "id1",
        "id2",
        "id3",
        "id4",
        "id5",
        "id6",
        "id7",
        "id8",
        "id9",
        "id10",
        "id11",
        "id12",
        "id13",
        "id14",
        "id15",
    ] * 2
    with pytest.raises(errors.DuplicateIDError, match="found 15 duplicated IDs: "):
        validate_ids(ids)
