import importlib

import pytest

from chromadb.utils.embedding_functions import TransformerEmbeddingFunction


@pytest.mark.skipif(
    importlib.util.find_spec("transformers") is None and importlib.util.find_spec("torch") is None,
    reason="test requires numba which is not installed"
)
def test_transformer_ef_default_mdoel():
    ef = TransformerEmbeddingFunction()
    embedding = ef(["text"])
    assert len(embedding[0]) == 384

@pytest.mark.skipif(
    importlib.util.find_spec("transformers") is None and importlib.util.find_spec("torch") is None,
    reason="test requires numba which is not installed"
)
def test_transformer_ef_custom_model():
    ef = TransformerEmbeddingFunction(model_name="dbmdz/bert-base-turkish-cased")
    embedding = ef(["Merhaba dünya", "Bu bir test cümlesidir"])
    assert embedding is not None
    assert len(embedding) == 2
    assert len(embedding[0]) == 768
