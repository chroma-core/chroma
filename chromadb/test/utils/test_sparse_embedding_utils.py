import pytest

from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector


def test_normalize_sparse_vector_keeps_values_aligned_when_sorting() -> None:
    vector = normalize_sparse_vector(
        indices=[3, 1, 2], values=[0.3, 0.1, 0.2], labels=["c", "a", "b"]
    )

    assert vector.indices == [1, 2, 3]
    assert vector.values == [0.1, 0.2, 0.3]
    assert vector.labels == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("indices", "values"),
    [([1, 2], [0.5]), ([1], [0.5, 0.25])],
)
def test_normalize_sparse_vector_rejects_mismatched_indices_and_values(
    indices: list[int], values: list[float]
) -> None:
    with pytest.raises(ValueError, match="indices and values have different lengths"):
        normalize_sparse_vector(indices=indices, values=values)


@pytest.mark.parametrize(
    ("indices", "labels"),
    [([1, 2], ["one"]), ([], ["orphaned"])],
)
def test_normalize_sparse_vector_rejects_mismatched_labels(
    indices: list[int], labels: list[str]
) -> None:
    with pytest.raises(ValueError, match="labels is provided and has different length"):
        normalize_sparse_vector(
            indices=indices, values=[0.5] * len(indices), labels=labels
        )
