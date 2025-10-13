import os
import pytest
from chromadb.utils.embedding_functions.voyageai_embedding_function import (
    VoyageAIEmbeddingFunction,
)

voyageai = pytest.importorskip("voyageai", reason="voyageai not installed")


def test_with_embedding_dimensions() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3.5",
        dimensions=2048,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 2048


def test_with_multimodal_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_multimodal_image_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_multimodal_mixed_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_contextual_embedding() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
        dimensions=2048,
    )
    embeddings = ef(["hello world", "in chroma"])
    assert embeddings is not None
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 2048


def test_count_tokens() -> None:
    """Test token counting functionality."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3",
    )
    texts = ["hello world", "this is a longer text with more tokens"]
    token_counts = ef.count_tokens(texts)
    assert len(token_counts) == 2
    assert token_counts[0] > 0
    assert token_counts[1] > token_counts[0]  # Longer text should have more tokens


def test_count_tokens_empty_list() -> None:
    """Test token counting with empty list."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3",
    )
    token_counts = ef.count_tokens([])
    assert token_counts == []


def test_count_tokens_single_text() -> None:
    """Test token counting with single text."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-2",
    )
    token_counts = ef.count_tokens(["hello"])
    assert len(token_counts) == 1
    assert token_counts[0] > 0


def test_get_token_limit() -> None:
    """Test getting token limit for different models."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")

    # Test voyage-2 model
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-2",
    )
    assert ef.get_token_limit() == 320_000

    # Test context model
    ef_context = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
    )
    assert ef_context.get_token_limit() == 32_000

    # Test voyage-3-large model
    ef_large = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3-large",
    )
    assert ef_large.get_token_limit() == 120_000


def test_token_counting_with_multimodal() -> None:
    """Test that token counting works with multimodal model."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
    )
    texts = ["hello world", "test text"]
    token_counts = ef.count_tokens(texts)
    assert len(token_counts) == 2
    assert all(count > 0 for count in token_counts)


def test_batching_with_batch_size() -> None:
    """Test that batching works with explicit batch_size parameter."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) > 0 for emb in embeddings)


def test_build_batches() -> None:
    """Test the _build_batches method."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-2",
        batch_size=2,
    )
    texts = ["short", "text", "here", "now"]
    batches = list(ef._build_batches(texts))
    # Should create 2 batches of 2 texts each
    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert len(batches[1]) == 2


def test_batching_with_large_texts() -> None:
    """Test batching with texts that exceed token limits."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3",
    )
    # Create long texts
    long_text = "This is a long text with many words. " * 100
    texts = [long_text, long_text, long_text]
    embeddings = ef(texts)
    assert len(embeddings) == 3
    assert all(len(emb) > 0 for emb in embeddings)


def test_config_includes_batch_size() -> None:
    """Test that config includes batch_size parameter."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-3",
        batch_size=10,
    )
    config = ef.get_config()
    assert "batch_size" in config
    assert config["batch_size"] == 10


def test_contextual_batching() -> None:
    """Test that contextual models support batching."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4"]
    embeddings = ef(texts)
    assert len(embeddings) == 4
    assert all(len(emb) > 0 for emb in embeddings)


def test_contextual_build_batches() -> None:
    """Test that contextual models use _build_batches correctly."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
        batch_size=3,
    )
    texts = ["short", "text", "here", "now", "more"]
    batches = list(ef._build_batches(texts))
    # Should create batches respecting batch_size=3
    assert len(batches) >= 2
    # First batch should have at most 3 items
    assert len(batches[0]) <= 3


def test_multimodal_text_only_batching() -> None:
    """Test that multimodal models support batching for text-only inputs."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) > 0 for emb in embeddings)


def test_contextual_with_large_batch() -> None:
    """Test contextual model with large batch that should be split."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-context-3",
        batch_size=5,
    )
    # Create many texts
    texts = [f"Document number {i} with some content" for i in range(15)]
    embeddings = ef(texts)
    assert len(embeddings) == 15
    assert all(len(emb) > 0 for emb in embeddings)


def test_multimodal_text_with_large_batch() -> None:
    """Test multimodal model with large text batch that should be split."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3",
        batch_size=3,
    )
    texts = [f"Text content {i}" for i in range(10)]
    embeddings = ef(texts)
    assert len(embeddings) == 10
    assert all(len(emb) > 0 for emb in embeddings)
