import pytest
from unittest.mock import MagicMock, patch
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
import numpy as np


@pytest.fixture(autouse=True)
def clear_models_cache():
    """Clear the class-level models cache before each test to ensure isolation."""
    SentenceTransformerEmbeddingFunction.models = {}


def test_initialization_no_pool():
    """Verify that a pool is not started by default."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        ef = SentenceTransformerEmbeddingFunction(model_name="test_model")
        mock_st.assert_called_once_with(model_name_or_path="test_model", device="cpu")
        assert ef._pool is None


def test_initialization_with_pool():
    """Verify that a multi-process pool is started when devices are specified."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = mock_st.return_value
        mock_instance.start_multi_process_pool.return_value = {"pool": "test_pool"}

        ef = SentenceTransformerEmbeddingFunction(
            model_name="test_model", multiprocess_devices=["cpu", "cpu"]
        )

        mock_instance.start_multi_process_pool.assert_called_once_with(
            target_devices=["cpu", "cpu"]
        )
        assert ef._pool == {"pool": "test_pool"}


def test_call_with_batch_size():
    """Verify that batch_size is passed to encode when specified."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = mock_st.return_value
        mock_instance.encode.return_value = [np.array([0.1, 0.2])]

        ef = SentenceTransformerEmbeddingFunction(model_name="test_model", batch_size=10)
        ef(["test doc"])

        mock_instance.encode.assert_called_once()
        args, kwargs = mock_instance.encode.call_args
        assert kwargs["batch_size"] == 10


def test_call_without_batch_size():
    """Verify that batch_size is NOT passed to encode when it is None."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = mock_st.return_value
        mock_instance.encode.return_value = [np.array([0.1, 0.2])]

        ef = SentenceTransformerEmbeddingFunction(model_name="test_model")
        ef(["test doc"])

        mock_instance.encode.assert_called_once()
        args, kwargs = mock_instance.encode.call_args
        assert "batch_size" not in kwargs


def test_call_with_pool():
    """Verify that the pool is passed to encode when multi-processing is enabled."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = mock_st.return_value
        mock_instance.start_multi_process_pool.return_value = {"pool": "test_pool"}
        mock_instance.encode.return_value = [np.array([0.1, 0.2])]

        ef = SentenceTransformerEmbeddingFunction(
            model_name="test_model", multiprocess_devices=["cpu"]
        )
        ef(["test doc"])

        args, kwargs = mock_instance.encode.call_args
        assert kwargs["pool"] == {"pool": "test_pool"}


def test_config_roundtrip():
    """Verify that getting config and rebuilding from it preserves all new parameters."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        ef = SentenceTransformerEmbeddingFunction(
            model_name="test_model",
            batch_size=10,
            multiprocess_devices=["cpu"],
            normalize_embeddings=True,
        )

        config = ef.get_config()
        assert config["batch_size"] == 10
        assert config["multiprocess_devices"] == ["cpu"]

        # Rebuild from config
        new_ef = SentenceTransformerEmbeddingFunction.build_from_config(config)
        assert new_ef.batch_size == 10
        assert new_ef.multiprocess_devices == ["cpu"]
        assert new_ef.normalize_embeddings is True


def test_cleanup():
    """Verify that deleting the instance stops the multi-process pool."""
    with patch("sentence_transformers.SentenceTransformer") as mock_st:
        mock_instance = mock_st.return_value
        mock_instance.start_multi_process_pool.return_value = {"pool": "test_pool"}

        ef = SentenceTransformerEmbeddingFunction(
            model_name="test_model", multiprocess_devices=["cpu"]
        )
        pool = ef._pool
        ef.__del__()

        mock_instance.stop_multi_process_pool.assert_called_once_with(pool)
