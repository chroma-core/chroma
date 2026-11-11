import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction


@pytest.fixture
def model_factory(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    factory = MagicMock(side_effect=lambda **kwargs: MagicMock())
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        SimpleNamespace(SentenceTransformer=factory),
    )
    monkeypatch.setattr(SentenceTransformerEmbeddingFunction, "models", {})
    return factory


def test_models_on_different_devices_are_not_shared(model_factory: MagicMock) -> None:
    cpu = SentenceTransformerEmbeddingFunction(model_name="test-model", device="cpu")
    gpu = SentenceTransformerEmbeddingFunction(model_name="test-model", device="cuda:0")

    assert cpu._model is not gpu._model
    assert model_factory.call_count == 2
    assert model_factory.call_args_list[1].kwargs["device"] == "cuda:0"


def test_same_model_and_device_reuse_cache(model_factory: MagicMock) -> None:
    first = SentenceTransformerEmbeddingFunction(model_name="test-model", device="cpu")
    second = SentenceTransformerEmbeddingFunction(model_name="test-model", device="cpu")

    assert first._model is second._model
    model_factory.assert_called_once_with(model_name_or_path="test-model", device="cpu")


def test_different_model_names_are_not_shared(model_factory: MagicMock) -> None:
    first = SentenceTransformerEmbeddingFunction(model_name="first-model", device="cpu")
    second = SentenceTransformerEmbeddingFunction(
        model_name="second-model", device="cpu"
    )

    assert first._model is not second._model
    assert model_factory.call_count == 2
