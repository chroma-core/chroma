import pytest

from chromadb.base_types import SparseVector
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector


def test_sorts_by_index() -> None:
    result = normalize_sparse_vector([3, 1, 2], [30.0, 10.0, 20.0])
    assert result == SparseVector(indices=[1, 2, 3], values=[10.0, 20.0, 30.0])


def test_sorts_labels_alongside() -> None:
    result = normalize_sparse_vector([2, 1], [20.0, 10.0], ["b", "a"])
    assert result == SparseVector(
        indices=[1, 2], values=[10.0, 20.0], labels=["a", "b"]
    )


def test_empty_returns_empty() -> None:
    assert normalize_sparse_vector([], []) == SparseVector(indices=[], values=[])


def test_more_indices_than_values_raises() -> None:
    # zip() would otherwise silently truncate to two elements, dropping index 3
    # instead of surfacing the mismatch the docstring promises.
    with pytest.raises(ValueError, match="same length"):
        normalize_sparse_vector([1, 2, 3], [10.0, 20.0])


def test_more_values_than_indices_raises() -> None:
    with pytest.raises(ValueError, match="same length"):
        normalize_sparse_vector([1, 2], [10.0, 20.0, 30.0])


def test_empty_indices_with_values_raises() -> None:
    with pytest.raises(ValueError, match="same length"):
        normalize_sparse_vector([], [10.0])


def test_labels_length_mismatch_raises() -> None:
    with pytest.raises(ValueError, match="labels must have the same length"):
        normalize_sparse_vector([1, 2], [10.0, 20.0], ["a"])
