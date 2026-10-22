from typing import Any
from unittest.mock import Mock

import numpy as np
import pytest

from chromadb.api.types import Metadatas
from chromadb.utils.batch_utils import create_batches


@pytest.mark.parametrize("field", ["embeddings", "metadatas", "documents"])
@pytest.mark.parametrize("count", [0, 2, 4])
@pytest.mark.parametrize("batch_size", [2, 3, 4])
def test_create_batches_rejects_misaligned_fields(
    field: str, count: int, batch_size: int
) -> None:
    api = Mock()
    api.get_max_batch_size.return_value = batch_size
    values: dict[str, Any] = {
        "embeddings": [np.array([1.0, 2.0])] * count,
        "metadatas": [{"source": "test"}] * count,
        "documents": ["document"] * count,
    }

    with pytest.raises(ValueError, match=field):
        create_batches(api, ["a", "b", "c"], **{field: values[field]})
    api.get_max_batch_size.assert_not_called()


@pytest.mark.parametrize("batch_size", [2, 3, 4])
def test_create_batches_preserves_aligned_fields(batch_size: int) -> None:
    api = Mock()
    api.get_max_batch_size.return_value = batch_size
    ids = ["a", "b", "c"]
    embeddings = [np.array([float(i), 0.0]) for i in range(3)]
    metadatas: Metadatas = [{"position": i} for i in range(3)]
    documents = [f"document {i}" for i in range(3)]

    batches = create_batches(api, ids, embeddings, metadatas, documents)

    assert [item for batch in batches for item in batch[0]] == ids
    assert [
        item for batch in batches if batch[2] is not None for item in batch[2]
    ] == metadatas
    assert [
        item for batch in batches if batch[3] is not None for item in batch[3]
    ] == documents
    actual_embeddings = [
        item for batch in batches if batch[1] is not None for item in batch[1]
    ]
    np.testing.assert_array_equal(actual_embeddings, embeddings)
    assert all(len(batch[0]) <= batch_size for batch in batches)


def test_create_batches_allows_omitted_fields() -> None:
    api = Mock()
    api.get_max_batch_size.return_value = 2

    assert create_batches(api, ["a", "b", "c"]) == [
        (["a", "b"], None, None, None),
        (["c"], None, None, None),
    ]


def test_create_batches_allows_empty_aligned_fields() -> None:
    api = Mock()
    api.get_max_batch_size.return_value = 2

    assert create_batches(api, [], [], [], []) == [([], [], [], [])]
