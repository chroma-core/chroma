from unittest.mock import MagicMock, patch
import numpy as np
from chromadb.utils.embedding_functions import HuggingFaceEmbeddingFunction
from typing import cast


def test_huggingface_embedding_functino() -> None:
    model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    api_key = "test-api-key"

    with patch("huggingface_hub.InferenceClient") as mock_client:
        mock_client_instance = MagicMock()
        mock_client.return_value = mock_client_instance

        mock_client_instance.feature_extraction.return_value = [
            [0.1, 0.2, 0.3],
            [0.4, 0.5, 0.6],
        ]

        ef = HuggingFaceEmbeddingFunction(
            api_key=api_key,
            model_name=model_name,
        )

        result = ef(["hello", "world"])

        mock_client.assert_called_once_with(
            provider="hf-inference",
            api_key=api_key,
        )

        mock_client_instance.feature_extraction.assert_called_once_with(
            ["hello", "world"],
            model=model_name,
        )

        assert len(result) == 2

        for embedding in result:
            embedding_array = cast(np.ndarray, embedding)
            assert isinstance(embedding_array, np.ndarray)
            assert embedding_array.dtype == np.float32

        np.testing.assert_array_equal(
            result[0],
            np.array([0.1, 0.2, 0.3], dtype=np.float32),
        )

        np.testing.assert_array_equal(
            result[1],
            np.array([0.4, 0.5, 0.6], dtype=np.float32),
        )


def test_huggingface_embedding_function_requires_huggingface_hub() -> None:
    with patch.dict("sys.modules", {"huggingface_hub": None}):
        try:
            HuggingFaceEmbeddingFunction(
                api_key="test-api-key",
            )
        except ValueError as e:
            assert "huggingface_hub" in str(e)
        else:
            raise AssertionError("Expected ValueError")
