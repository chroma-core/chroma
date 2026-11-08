import pytest

from chromadb.base_types import SparseVector
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector


def test_normalize_sparse_vector_sorts_indices_and_values_together() -> None:
    result = normalize_sparse_vector(indices=[3, 1, 2], values=[0.3, 0.1, 0.2])

    assert isinstance(result, SparseVector)
    assert result.indices == [1, 2, 3]
    assert result.values == [0.1, 0.2, 0.3]
    assert result.labels is None


def test_normalize_sparse_vector_sorts_labels_with_their_index() -> None:
    result = normalize_sparse_vector(
        indices=[3, 1], values=[0.3, 0.1], labels=["c", "a"]
    )

    assert result.indices == [1, 3]
    assert result.values == [0.1, 0.3]
    assert result.labels == ["a", "c"]


def test_normalize_sparse_vector_returns_empty_vector() -> None:
    result = normalize_sparse_vector(indices=[], values=[])

    assert result.indices == []
    assert result.values == []
    assert result.labels is None


@pytest.mark.parametrize(
    "indices, values",
    [
        ([1, 2, 3], [0.5]),
        ([1], [0.5, 0.25, 0.125]),
        ([], [0.5]),
    ],
)
def test_normalize_sparse_vector_rejects_mismatched_indices_and_values(
    indices: list, values: list
) -> None:
    with pytest.raises(
        ValueError, match="indices and values must have the same length"
    ):
        normalize_sparse_vector(indices=indices, values=values)


@pytest.mark.parametrize(
    "indices, values, labels",
    [
        ([1, 2, 3], [0.5, 0.25, 0.125], ["a", "b"]),
        ([1, 2], [0.5, 0.25], ["a", "b", "c"]),
    ],
)
def test_normalize_sparse_vector_rejects_mismatched_labels(
    indices: list, values: list, labels: list
) -> None:
    with pytest.raises(ValueError, match="labels must have the same length"):
        normalize_sparse_vector(indices=indices, values=values, labels=labels)
