import numpy as np

from chromadb.utils.embedding_functions.mock_embedding_function import (
    MockEmbeddingFunction,
)
from chromadb.utils.embedding_functions.mock_sparse_embedding_function import (
    MockSparseEmbeddingFunction,
)


# --- MockEmbeddingFunction tests ---


def test_mock_ef_generates_correct_dimension() -> None:
    ef = MockEmbeddingFunction(dim=128)
    embeddings = ef(["hello"])
    assert len(embeddings) == 1
    assert embeddings[0].shape == (128,)


def test_mock_ef_default_dimension() -> None:
    ef = MockEmbeddingFunction()
    embeddings = ef(["hello"])
    assert embeddings[0].shape == (256,)


def test_mock_ef_deterministic() -> None:
    ef = MockEmbeddingFunction(dim=64)
    a = ef(["hello world"])
    b = ef(["hello world"])
    assert np.array_equal(a[0], b[0])


def test_mock_ef_different_inputs_produce_different_outputs() -> None:
    ef = MockEmbeddingFunction(dim=64)
    a = ef(["hello"])
    b = ef(["world"])
    assert not np.array_equal(a[0], b[0])


def test_mock_ef_multiple_inputs() -> None:
    ef = MockEmbeddingFunction(dim=32)
    embeddings = ef(["a", "b", "c"])
    assert len(embeddings) == 3
    for emb in embeddings:
        assert emb.shape == (32,)


def test_mock_ef_seed_changes_output() -> None:
    ef1 = MockEmbeddingFunction(dim=64, seed="seed1")
    ef2 = MockEmbeddingFunction(dim=64, seed="seed2")
    a = ef1(["hello"])
    b = ef2(["hello"])
    assert not np.array_equal(a[0], b[0])


def test_mock_ef_config_roundtrip() -> None:
    ef = MockEmbeddingFunction(dim=128, seed="test")
    config = ef.get_config()
    restored = MockEmbeddingFunction.build_from_config(config)
    assert restored._dim == 128
    assert restored._seed == "test"

    original = ef(["test"])
    rebuilt = restored(["test"])
    assert np.array_equal(original[0], rebuilt[0])


def test_mock_ef_name() -> None:
    assert MockEmbeddingFunction.name() == "mock"


def test_mock_ef_dtype() -> None:
    ef = MockEmbeddingFunction(dim=16, dtype=np.float64)
    embeddings = ef(["hello"])
    assert embeddings[0].dtype == np.float64


# --- MockSparseEmbeddingFunction tests ---


def test_mock_sparse_ef_generates_sparse_vectors() -> None:
    ef = MockSparseEmbeddingFunction()
    vectors = ef(["hello"])
    assert len(vectors) == 1
    assert len(vectors[0].indices) > 0
    assert len(vectors[0].indices) == len(vectors[0].values)


def test_mock_sparse_ef_deterministic() -> None:
    ef = MockSparseEmbeddingFunction()
    a = ef(["hello world"])
    b = ef(["hello world"])
    assert a[0].indices == b[0].indices
    assert a[0].values == b[0].values


def test_mock_sparse_ef_different_inputs() -> None:
    ef = MockSparseEmbeddingFunction()
    a = ef(["hello"])
    b = ef(["world"])
    assert a[0].indices != b[0].indices


def test_mock_sparse_ef_multiple_inputs() -> None:
    ef = MockSparseEmbeddingFunction()
    vectors = ef(["a", "b", "c"])
    assert len(vectors) == 3


def test_mock_sparse_ef_indices_sorted() -> None:
    ef = MockSparseEmbeddingFunction()
    vectors = ef(["test sorting"])
    indices = vectors[0].indices
    assert indices == sorted(indices)
    # Strictly ascending (no duplicates)
    for i in range(1, len(indices)):
        assert indices[i] > indices[i - 1]


def test_mock_sparse_ef_indices_within_vocab() -> None:
    ef = MockSparseEmbeddingFunction(vocab_size=100)
    vectors = ef(["hello world"])
    for idx in vectors[0].indices:
        assert 0 <= idx < 100


def test_mock_sparse_ef_seed_changes_output() -> None:
    ef1 = MockSparseEmbeddingFunction(seed="s1")
    ef2 = MockSparseEmbeddingFunction(seed="s2")
    a = ef1(["hello"])
    b = ef2(["hello"])
    assert a[0].indices != b[0].indices


def test_mock_sparse_ef_config_roundtrip() -> None:
    ef = MockSparseEmbeddingFunction(vocab_size=500, nnz=10, seed="test")
    config = ef.get_config()
    restored = MockSparseEmbeddingFunction.build_from_config(config)
    assert restored._vocab_size == 500
    assert restored._nnz == 10
    assert restored._seed == "test"

    original = ef(["test"])
    rebuilt = restored(["test"])
    assert original[0].indices == rebuilt[0].indices
    assert original[0].values == rebuilt[0].values


def test_mock_sparse_ef_name() -> None:
    assert MockSparseEmbeddingFunction.name() == "mock_sparse"
