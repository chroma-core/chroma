"""Tests for SPLADE sparse embedding chunking and max pooling."""

import pytest

from chromadb.base_types import SparseVector
from chromadb.utils.sparse_embedding_utils import max_pool_sparse_vectors
from chromadb.utils.embedding_functions.chroma_cloud_splade_embedding_function import (
    ChromaCloudSpladeEmbeddingFunction,
)


# ---------------------------------------------------------------------------
# max_pool_sparse_vectors tests
# ---------------------------------------------------------------------------


class TestMaxPoolSparseVectors:
    def test_single_vector_returned_as_is(self) -> None:
        vec = SparseVector(indices=[1, 3, 5], values=[0.1, 0.2, 0.3])
        result = max_pool_sparse_vectors([vec])
        assert result is vec

    def test_empty_list_raises(self) -> None:
        with pytest.raises(ValueError, match="empty"):
            max_pool_sparse_vectors([])

    def test_max_pool_two_vectors_disjoint_indices(self) -> None:
        v1 = SparseVector(indices=[1, 3], values=[0.5, 0.8])
        v2 = SparseVector(indices=[2, 4], values=[0.6, 0.9])
        result = max_pool_sparse_vectors([v1, v2])
        assert result.indices == [1, 2, 3, 4]
        assert result.values == [0.5, 0.6, 0.8, 0.9]

    def test_max_pool_overlapping_indices_takes_max(self) -> None:
        v1 = SparseVector(indices=[1, 2, 3], values=[0.5, 0.8, 0.1])
        v2 = SparseVector(indices=[1, 2, 3], values=[0.3, 0.9, 0.4])
        result = max_pool_sparse_vectors([v1, v2])
        assert result.indices == [1, 2, 3]
        assert result.values == [0.5, 0.9, 0.4]

    def test_max_pool_partial_overlap(self) -> None:
        v1 = SparseVector(indices=[1, 3, 5], values=[0.5, 0.8, 0.1])
        v2 = SparseVector(indices=[2, 3, 6], values=[0.6, 0.2, 0.9])
        result = max_pool_sparse_vectors([v1, v2])
        assert result.indices == [1, 2, 3, 5, 6]
        assert result.values == [0.5, 0.6, 0.8, 0.1, 0.9]

    def test_max_pool_three_vectors(self) -> None:
        v1 = SparseVector(indices=[1, 2], values=[0.1, 0.5])
        v2 = SparseVector(indices=[1, 3], values=[0.3, 0.6])
        v3 = SparseVector(indices=[2, 3], values=[0.9, 0.2])
        result = max_pool_sparse_vectors([v1, v2, v3])
        assert result.indices == [1, 2, 3]
        assert result.values == [0.3, 0.9, 0.6]

    def test_max_pool_with_labels(self) -> None:
        v1 = SparseVector(
            indices=[1, 2], values=[0.5, 0.3], labels=["hello", "world"]
        )
        v2 = SparseVector(
            indices=[1, 2], values=[0.3, 0.8], labels=["hello", "earth"]
        )
        result = max_pool_sparse_vectors([v1, v2])
        assert result.indices == [1, 2]
        assert result.values == [0.5, 0.8]
        assert result.labels == ["hello", "earth"]

    def test_max_pool_without_labels_returns_none(self) -> None:
        v1 = SparseVector(indices=[1], values=[0.5])
        v2 = SparseVector(indices=[1], values=[0.3])
        result = max_pool_sparse_vectors([v1, v2])
        assert result.labels is None

    def test_max_pool_result_indices_sorted(self) -> None:
        v1 = SparseVector(indices=[100, 200], values=[0.1, 0.2])
        v2 = SparseVector(indices=[50, 150], values=[0.3, 0.4])
        result = max_pool_sparse_vectors([v1, v2])
        assert result.indices == sorted(result.indices)


# ---------------------------------------------------------------------------
# _chunk_text_by_chars tests (does not require network / tokenizer download)
# ---------------------------------------------------------------------------


class TestChunkTextByChars:
    def test_short_text_not_chunked(self) -> None:
        text = "This is a short document."
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(text)
        assert chunks == [text]

    def test_long_text_is_chunked(self) -> None:
        words = ["embedding"] * 600
        text = " ".join(words)
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(text)
        assert len(chunks) > 1

        for chunk in chunks:
            assert chunk in text

    def test_chunks_cover_full_text(self) -> None:
        """All content from the original text should appear in some chunk."""
        words = [f"word{i}" for i in range(800)]
        text = " ".join(words)
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(text)

        all_chunk_text = " ".join(chunks)
        for word in words:
            assert word in all_chunk_text

    def test_empty_string_returns_single_chunk(self) -> None:
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars("")
        assert len(chunks) == 1

    def test_splits_on_word_boundaries(self) -> None:
        text = "word " * 600
        text = text.strip()
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(text)
        for chunk in chunks:
            assert not chunk.startswith(" ")
            assert not chunk.endswith(" ")


# ---------------------------------------------------------------------------
# Full pipeline: chunking + max pooling
# ---------------------------------------------------------------------------


class TestChunkingPipeline:
    """Test the chunking + max-pooling pipeline logic end-to-end.

    These tests exercise the same code path as __call__ but without
    instantiating the full embedding function (which requires API keys
    and HTTP calls).
    """

    def test_short_doc_produces_one_chunk(self) -> None:
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(
            "short doc"
        )
        assert len(chunks) == 1

    def test_long_doc_chunked_and_max_pooled(self) -> None:
        """Simulate the __call__ pipeline for a long document."""
        long_text = "word " * 600
        chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(
            long_text.strip()
        )
        assert len(chunks) > 1

        # Simulate embeddings for each chunk
        chunk_embeddings = [
            SparseVector(indices=[1, 2], values=[0.5, 0.3]),
            SparseVector(indices=[2, 3], values=[0.8, 0.6]),
        ]
        # Only use as many as we have chunks (or pad for testing)
        chunk_embeddings = chunk_embeddings[: len(chunks)]

        if len(chunk_embeddings) > 1:
            result = max_pool_sparse_vectors(chunk_embeddings)
            # Max pool: idx 1 -> 0.5, idx 2 -> max(0.3, 0.8) = 0.8, idx 3 -> 0.6
            assert result.indices == [1, 2, 3]
            assert result.values == [0.5, 0.8, 0.6]

    def test_multiple_docs_tracked_independently(self) -> None:
        """Verify the chunk-to-doc mapping logic used in __call__."""
        docs = ["short", "word " * 600]
        all_chunks = []
        doc_chunk_ranges = []
        for doc in docs:
            start = len(all_chunks)
            chunks = ChromaCloudSpladeEmbeddingFunction._chunk_text_by_chars(
                doc.strip()
            )
            all_chunks.extend(chunks)
            doc_chunk_ranges.append((start, len(all_chunks)))

        # First doc: 1 chunk
        assert doc_chunk_ranges[0] == (0, 1)
        # Second doc: >1 chunks
        assert doc_chunk_ranges[1][1] - doc_chunk_ranges[1][0] > 1
