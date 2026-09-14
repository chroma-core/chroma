import os
import tempfile
from typing import Dict, Any

import numpy as np
from numpy.typing import NDArray
import pytest
import onnxruntime
from unittest.mock import patch, MagicMock

from chromadb.utils.embedding_functions import ONNXMiniLM_L6_V2, EmbeddingFunction


class TestONNXMiniLM_L6_V2:
    """Test suite for ONNXMiniLM_L6_V2 embedding function."""

    def test_initialization(self) -> None:
        """Test that the embedding function initializes correctly."""
        ef = ONNXMiniLM_L6_V2()
        assert ef is not None
        assert isinstance(ef, EmbeddingFunction)

        # Test with valid providers
        available_providers = onnxruntime.get_available_providers()
        if available_providers:
            ef = ONNXMiniLM_L6_V2(preferred_providers=[available_providers[0]])
            assert ef is not None

        # Test with None providers
        ef = ONNXMiniLM_L6_V2(preferred_providers=None)
        assert ef is not None

    def test_embedding_shape_and_normalization(self) -> None:
        """Test that embeddings have the correct shape and are normalized."""
        ef = ONNXMiniLM_L6_V2()

        # Test with a single document
        docs = ["This is a test document"]
        embeddings = ef(docs)

        # Check shape and type
        assert isinstance(embeddings, list)
        assert len(embeddings) == 1
        assert (
            len(embeddings[0]) == 384
        )  # MiniLM-L6-v2 produces 384-dimensional embeddings

        # Check normalization (for cosine similarity)
        embedding_np = np.array(embeddings[0])
        norm = np.linalg.norm(embedding_np)
        assert np.isclose(norm, 1.0, atol=1e-5)

        # Test with multiple documents
        docs = ["First document", "Second document", "Third document"]
        embeddings = ef(docs)

        # Check shape
        assert len(embeddings) == 3
        assert all(len(emb) == 384 for emb in embeddings)

    def test_batch_processing(self) -> None:
        """Test that the embedding function correctly processes batches."""
        ef = ONNXMiniLM_L6_V2()

        # Create a list of documents larger than the default batch size (32)
        docs = [f"Document {i}" for i in range(40)]

        # Get embeddings
        embeddings = ef(docs)

        # Check that all documents were processed
        assert len(embeddings) == 40
        assert all(len(emb) == 384 for emb in embeddings)

    def test_config_serialization(self) -> None:
        """Test that the embedding function can be serialized and deserialized."""
        # Create an embedding function with specific providers
        available_providers = onnxruntime.get_available_providers()
        providers = available_providers[:1] if available_providers else None
        ef = ONNXMiniLM_L6_V2(preferred_providers=providers)

        # Get config
        config = ef.get_config()

        # Check config
        assert isinstance(config, dict)
        assert "preferred_providers" in config

        # Build from config
        ef2 = ONNXMiniLM_L6_V2.build_from_config(config)

        # Check that the new instance works
        docs = ["Test document"]
        embeddings = ef2(docs)
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 384

    def test_max_tokens(self) -> None:
        """Test the max_tokens method."""
        ef = ONNXMiniLM_L6_V2()
        assert ef.max_tokens() == 256  # Default for this model

    @patch("httpx.stream")
    def test_download_functionality(self, mock_stream: MagicMock) -> None:
        """Test the model download functionality with mocking."""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.headers.get.return_value = "1000"
        mock_response.iter_bytes.return_value = [b"test data"]
        mock_stream.return_value.__enter__.return_value = mock_response

        # Create a temporary directory for testing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Patch the download path
            with patch.object(ONNXMiniLM_L6_V2, "DOWNLOAD_PATH", temp_dir):
                with patch(
                    "chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2._verify_sha256",
                    return_value=True,
                ):
                    ef = ONNXMiniLM_L6_V2()
                    # Call download method directly
                    ef._download(
                        url="https://test.url",
                        fname=os.path.join(temp_dir, "test_file"),
                    )

                    # Check that the file was created
                    assert os.path.exists(os.path.join(temp_dir, "test_file"))

    def test_validate_config(self) -> None:
        """Test config validation."""
        ef = ONNXMiniLM_L6_V2()

        # Test validate_config
        config: Dict[str, Any] = {"preferred_providers": ["CPUExecutionProvider"]}
        ef.validate_config(config)  # Should not raise

        # Test validate_config_update
        old_config: Dict[str, Any] = {"preferred_providers": ["CPUExecutionProvider"]}
        new_config: Dict[str, Any] = {"preferred_providers": ["CUDAExecutionProvider"]}
        ef.validate_config_update(old_config, new_config)  # Should not raise

    @pytest.mark.parametrize(
        "input_text",
        [
            "Short text",
            "A longer text that contains multiple words and should be embedded properly",
            "",  # Empty string
            "Special characters: !@#$%^&*()",
            "Numbers: 1234567890",
            "Unicode: 你好, こんにちは, 안녕하세요",
        ],
    )
    def test_various_inputs(self, input_text: str) -> None:
        """Test the embedding function with various types of input text."""
        ef = ONNXMiniLM_L6_V2()

        # Get embeddings
        embeddings = ef([input_text])

        # Check that embeddings were generated
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 384

    def test_consistency(self) -> None:
        """Test that the embedding function produces consistent results."""
        ef = ONNXMiniLM_L6_V2()

        # Get embeddings for the same text twice
        text = "This is a test document"
        embeddings1 = ef([text])
        embeddings2 = ef([text])

        # Check that the embeddings are the same
        np.testing.assert_allclose(embeddings1[0], embeddings2[0])

    def test_similar_texts_have_similar_embeddings(self) -> None:
        """Test that similar texts have similar embeddings."""
        ef = ONNXMiniLM_L6_V2()

        # Get embeddings for similar texts
        text1 = "The cat sat on the mat"
        text2 = "A cat was sitting on a mat"
        text3 = "Quantum physics is fascinating"

        embeddings = ef([text1, text2, text3])

        # Calculate cosine similarities
        def cosine_similarity(a: NDArray[np.float32], b: NDArray[np.float32]) -> float:
            return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))

        # Similar texts should have higher similarity
        sim_1_2 = cosine_similarity(
            np.array(embeddings[0], dtype=np.float32),
            np.array(embeddings[1], dtype=np.float32),
        )
        sim_1_3 = cosine_similarity(
            np.array(embeddings[0], dtype=np.float32),
            np.array(embeddings[2], dtype=np.float32),
        )

        # The similarity between text1 and text2 should be higher than between text1 and text3
        assert sim_1_2 > sim_1_3

    @patch.object(ONNXMiniLM_L6_V2, "_download_model_if_not_exists")
    @patch.object(ONNXMiniLM_L6_V2, "_forward")
    def test_single_string_input_normalizes_to_single_embedding(
        self, mock_forward: MagicMock, mock_download: MagicMock
    ) -> None:
        ef = ONNXMiniLM_L6_V2()
        text = "hello world"

        mock_forward.side_effect = lambda docs: np.ones(
            (len(docs), 384), dtype=np.float32
        )

        single_str_result = ef(text)  # type: ignore[arg-type]
        assert mock_forward.call_args[0][0] == [text]

        list_result = ef([text])
        assert isinstance(single_str_result, list)
        assert len(single_str_result) == 1
        assert len(single_str_result[0]) == 384

        np.testing.assert_allclose(single_str_result[0], list_result[0], atol=1e-5)

    @patch.object(ONNXMiniLM_L6_V2, "_download_model_if_not_exists")
    @patch.object(ONNXMiniLM_L6_V2, "_forward")
    def test_embed_query_with_single_string(
        self, mock_forward: MagicMock, mock_download: MagicMock
    ) -> None:
        ef = ONNXMiniLM_L6_V2()
        query = "search query"
        mock_forward.side_effect = lambda docs: np.ones(
            (len(docs), 384), dtype=np.float32
        )

        embeddings = ef.embed_query(query)  # type: ignore[arg-type]
        assert mock_forward.call_args[0][0] == [query]
        assert isinstance(embeddings, list)
        assert len(embeddings) == 1
        assert len(embeddings[0]) == 384

    def test_invalid_input_types_raise_value_error(self) -> None:
        ef = ONNXMiniLM_L6_V2()

        with pytest.raises(ValueError, match="Expected input to be a non-empty string"):
            ef(None)  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="Expected input to be a list or str"):
            ef(12345)  # type: ignore[arg-type]

        with pytest.raises(ValueError, match="Expected document to be a str"):
            ef(["valid document", 999])  # type: ignore[list-item]
