import pytest

from chromadb.base_types import (
    SPARSE_VECTOR_TYPE_VALUE,
    TYPE_KEY,
    SparseVector,
)


def test_sparse_vector_from_dict_missing_indices_raises_value_error() -> None:
    """A sparse-vector dict missing 'indices' must raise ValueError, not KeyError."""
    with pytest.raises(ValueError, match="indices"):
        SparseVector.from_dict(
            {TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE, "values": [1.0, 2.0]}
        )


def test_sparse_vector_from_dict_missing_values_raises_value_error() -> None:
    """A sparse-vector dict missing 'values' must raise ValueError, not KeyError."""
    with pytest.raises(ValueError, match="values"):
        SparseVector.from_dict({TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE, "indices": [0, 1]})


def test_sparse_vector_from_dict_empty_sparse_vector_dict_raises_value_error() -> None:
    """A dict carrying only the sparse-vector type tag must raise ValueError."""
    with pytest.raises(ValueError, match="indices"):
        SparseVector.from_dict({TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE})


def test_sparse_vector_from_dict_valid_dict_unchanged() -> None:
    """Valid sparse-vector dicts (incl. 'tokens' -> labels) still deserialize."""
    sparse = SparseVector.from_dict(
        {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": [0, 2, 5],
            "values": [1.0, 2.0, 3.0],
            "tokens": ["a", "b", "c"],
        }
    )
    assert sparse.indices == [0, 2, 5]
    assert sparse.values == [1.0, 2.0, 3.0]
    assert sparse.labels == ["a", "b", "c"]
