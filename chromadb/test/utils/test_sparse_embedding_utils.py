import pytest

from chromadb.base_types import SparseVector
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector


def test_normalize_sorts_indices() -> None:
    result = normalize_sparse_vector(indices=[3, 1, 2], values=[30.0, 10.0, 20.0])
    assert result.indices == [1, 2, 3]
    assert result.values == [10.0, 20.0, 30.0]
    assert result.labels is None


def test_normalize_sorts_indices_with_labels() -> None:
    result = normalize_sparse_vector(
        indices=[2, 0, 1],
        values=[20.0, 0.0, 10.0],
        labels=["c", "a", "b"],
    )
    assert result.indices == [0, 1, 2]
    assert result.values == [0.0, 10.0, 20.0]
    assert result.labels == ["a", "b", "c"]


def test_normalize_empty() -> None:
    result = normalize_sparse_vector(indices=[], values=[])
    assert result.indices == []
    assert result.values == []
    assert result.labels is None


def test_normalize_raises_on_indices_values_length_mismatch() -> None:
    # The docstring promises a ValueError, but zip() silently truncates the
    # longer list, dropping data. More values than indices.
    with pytest.raises(ValueError):
        normalize_sparse_vector(indices=[1, 2], values=[10.0, 20.0, 30.0])


def test_normalize_raises_on_more_indices_than_values() -> None:
    with pytest.raises(ValueError):
        normalize_sparse_vector(indices=[1, 2, 3], values=[10.0, 20.0])


def test_normalize_raises_on_labels_length_mismatch() -> None:
    with pytest.raises(ValueError):
        normalize_sparse_vector(
            indices=[1, 2], values=[10.0, 20.0], labels=["only-one"]
        )


def test_normalize_does_not_silently_drop_data() -> None:
    # Regression: zip() truncation used to silently drop the extra index,
    # producing a 2-element SparseVector instead of raising.
    try:
        result = normalize_sparse_vector(indices=[1, 2, 3], values=[10.0, 20.0])
    except ValueError:
        return
    assert isinstance(result, SparseVector)
    pytest.fail(
        "Expected ValueError on mismatched lengths; got "
        f"{result!r} (data silently dropped)"
    )
