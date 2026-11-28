import pytest

from chromadb.base_types import SparseVector
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector


def test_normalize_sparse_vector_sorts_without_labels() -> None:
    result = normalize_sparse_vector(indices=[5, 1, 3], values=[0.5, 0.1, 0.3])

    assert result == SparseVector(
        indices=[1, 3, 5],
        values=[0.1, 0.3, 0.5],
        labels=None,
    )


def test_normalize_sparse_vector_sorts_with_labels() -> None:
    result = normalize_sparse_vector(
        indices=[5, 1, 3],
        values=[0.5, 0.1, 0.3],
        labels=["five", "one", "three"],
    )

    assert result == SparseVector(
        indices=[1, 3, 5],
        values=[0.1, 0.3, 0.5],
        labels=["one", "three", "five"],
    )


def test_normalize_sparse_vector_empty_input() -> None:
    expected = SparseVector(indices=[], values=[], labels=None)

    assert normalize_sparse_vector(indices=[], values=[]) == expected
    assert normalize_sparse_vector(indices=[], values=[], labels=[]) == expected


def test_normalize_sparse_vector_more_indices_than_values_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector indices and values must have the same length, "
            "got 3 indices and 1 values"
        ),
    ):
        normalize_sparse_vector(indices=[1, 2, 3], values=[0.5])


def test_normalize_sparse_vector_more_values_than_indices_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector indices and values must have the same length, "
            "got 1 indices and 3 values"
        ),
    ):
        normalize_sparse_vector(indices=[1], values=[0.5, 0.25, 0.125])


def test_normalize_sparse_vector_labels_shorter_than_indices_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector labels must have the same length as indices and values, "
            "got 2 labels, 3 indices"
        ),
    ):
        normalize_sparse_vector(
            indices=[1, 2, 3],
            values=[0.5, 0.25, 0.125],
            labels=["a", "b"],
        )

    with pytest.raises(
        ValueError,
        match=(
            "SparseVector labels must have the same length as indices and values, "
            "got 0 labels, 2 indices"
        ),
    ):
        normalize_sparse_vector(
            indices=[1, 2],
            values=[0.5, 0.25],
            labels=[],
        )


def test_normalize_sparse_vector_labels_longer_than_indices_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector labels must have the same length as indices and values, "
            "got 3 labels, 2 indices"
        ),
    ):
        normalize_sparse_vector(
            indices=[1, 2],
            values=[0.5, 0.25],
            labels=["a", "b", "c"],
        )


def test_normalize_sparse_vector_empty_indices_with_non_empty_values_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector indices and values must have the same length, "
            "got 0 indices and 1 values"
        ),
    ):
        normalize_sparse_vector(indices=[], values=[0.5])


def test_normalize_sparse_vector_empty_indices_with_non_empty_labels_raises() -> None:
    with pytest.raises(
        ValueError,
        match=(
            "SparseVector labels must have the same length as indices and values, "
            "got 1 labels, 0 indices"
        ),
    ):
        normalize_sparse_vector(indices=[], values=[], labels=["a"])
