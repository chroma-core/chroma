import os
import pytest
from typing import Any, Optional
from unittest.mock import Mock, patch
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
    """Test batching behavior through the public API without requiring API key."""
    with patch("voyageai.Client") as mock_client_class:
        # Create mock client instance
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock tokenize to return token lists
        mock_client.tokenize.return_value = [[1, 2], [3, 4], [5, 6], [7, 8]]

        # Mock embed to return embeddings
        def mock_embed(
            texts: list[str],
            model: str,
            input_type: Optional[str] = None,
            truncation: bool = True,
            output_dimension: Optional[int] = None,
        ) -> Mock:
            result = Mock()
            result.embeddings = [[0.1] * 1024 for _ in texts]
            return result

        mock_client.embed.side_effect = mock_embed

        ef = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-2",
            batch_size=2,
        )

        texts = ["short", "text", "here", "now"]
        embeddings = ef(texts)

        # Should return correct number of embeddings
        assert len(embeddings) == 4
        assert all(len(emb) > 0 for emb in embeddings)

        # Verify embed was called with batches of size 2
        assert mock_client.embed.call_count == 2


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
    """Test that contextual models handle batching correctly through the public API without requiring API key."""
    with patch("voyageai.Client") as mock_client_class:
        # Create mock client instance
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Mock tokenize to return token lists
        mock_client.tokenize.return_value = [[1, 2], [3, 4], [5, 6], [7, 8], [9, 10]]

        # Mock contextualized_embed to return embeddings
        def mock_contextualized_embed(
            inputs: list[Any],
            model: str,
            input_type: Optional[str] = None,
            output_dimension: Optional[int] = None,
        ) -> Mock:
            result = Mock()
            # For contextual models, inputs is a list of batches
            batch_results = []
            for batch in inputs:
                batch_result = Mock()
                batch_result.embeddings = [[0.1] * 2048 for _ in batch]
                batch_results.append(batch_result)
            result.results = batch_results
            return result

        mock_client.contextualized_embed.side_effect = mock_contextualized_embed

        ef = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-context-3",
            batch_size=3,
        )

        texts = ["short", "text", "here", "now", "more"]
        embeddings = ef(texts)

        # Should return correct number of embeddings
        assert len(embeddings) == 5
        assert all(len(emb) > 0 for emb in embeddings)

        # Verify contextualized_embed was called (should batch into 3+2)
        assert mock_client.contextualized_embed.call_count == 2


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


def test_with_multimodal_3_5_embeddings() -> None:
    """Test voyage-multimodal-3.5 model with text embeddings."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3.5",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024  # Default dimension for voyage-multimodal-3.5


def test_multimodal_3_5_token_limit() -> None:
    """Test that voyage-multimodal-3.5 has correct token limit."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3.5",
    )
    assert ef.get_token_limit() == 32_000


def test_multimodal_3_5_text_batching() -> None:
    """Test voyage-multimodal-3.5 model with text batching."""
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-multimodal-3.5",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) > 0 for emb in embeddings)


def test_voyage_4_family_token_limits() -> None:
    """Test getting token limit for voyage-4 family models."""
    with patch("voyageai.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        # Test voyage-4 model
        ef = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-4",
        )
        assert ef.get_token_limit() == 320_000

        # Test voyage-4-lite model
        ef_lite = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-4-lite",
        )
        assert ef_lite.get_token_limit() == 1_000_000

        # Test voyage-4-large model
        ef_large = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-4-large",
        )
        assert ef_large.get_token_limit() == 120_000


def test_voyage_4_build_batches() -> None:
    """Test batching behavior for voyage-4 model through the public API."""
    with patch("voyageai.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        mock_client.tokenize.return_value = [[1, 2], [3, 4], [5, 6]]

        def mock_embed(
            texts: list[str],
            model: str,
            input_type: Optional[str] = None,
            truncation: bool = True,
            output_dimension: Optional[int] = None,
        ) -> Mock:
            result = Mock()
            result.embeddings = [[0.1] * 1024 for _ in texts]
            return result

        mock_client.embed.side_effect = mock_embed

        ef = VoyageAIEmbeddingFunction(
            api_key="test-key",
            model_name="voyage-4",
            batch_size=2,
        )

        texts = ["text1", "text2", "text3"]
        embeddings = ef(texts)

        assert len(embeddings) == 3
        assert all(len(emb) == 1024 for emb in embeddings)
        assert mock_client.embed.call_count == 2


def test_voyage_4_family_not_context_or_multimodal() -> None:
    """Test that voyage-4 family models are not detected as context or multimodal."""
    with patch("voyageai.Client") as mock_client_class:
        mock_client = Mock()
        mock_client_class.return_value = mock_client

        for model_name in ["voyage-4", "voyage-4-lite", "voyage-4-large"]:
            ef = VoyageAIEmbeddingFunction(
                api_key="test-key",
                model_name=model_name,
            )
            assert ef._is_context_model() is False
            assert ef._is_multimodal_model() is False


def test_with_voyage_4_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_voyage_4_custom_dimensions() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4",
        dimensions=2048,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 2048


def test_voyage_4_batching() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) == 1024 for emb in embeddings)


def test_with_voyage_4_lite_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-lite",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_voyage_4_lite_custom_dimensions() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-lite",
        dimensions=512,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 512


def test_voyage_4_lite_batching() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-lite",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) == 1024 for emb in embeddings)


def test_with_voyage_4_large_embeddings() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-large",
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 1024


def test_with_voyage_4_large_custom_dimensions() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-large",
        dimensions=256,
    )
    embeddings = ef(["hello world"])
    assert embeddings is not None
    assert len(embeddings) == 1
    assert len(embeddings[0]) == 256


def test_voyage_4_large_batching() -> None:
    if os.environ.get("CHROMA_VOYAGE_API_KEY") is None:
        pytest.skip("CHROMA_VOYAGE_API_KEY not set")
    ef = VoyageAIEmbeddingFunction(
        api_key=os.environ["CHROMA_VOYAGE_API_KEY"],
        model_name="voyage-4-large",
        batch_size=2,
    )
    texts = ["text1", "text2", "text3", "text4", "text5"]
    embeddings = ef(texts)
    assert len(embeddings) == 5
    assert all(len(emb) == 1024 for emb in embeddings)
