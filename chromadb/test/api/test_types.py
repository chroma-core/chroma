import pytest
import threading
import time
from typing import List, cast, Dict, Any
from chromadb.api.types import Documents, Image, Document, Embeddings
from chromadb.utils.embedding_functions import (
    EmbeddingFunction,
    register_embedding_function,
)
import numpy as np


def random_embeddings() -> Embeddings:
    return cast(
        Embeddings, [embedding for embedding in np.random.random(size=(10, 10))]
    )


def random_image() -> Image:
    return np.random.randint(0, 255, size=(10, 10, 3), dtype=np.int64)


def random_documents() -> List[Document]:
    return [str(random_image()) for _ in range(10)]


def test_embedding_function_results_format_when_response_is_valid() -> None:
    valid_embeddings = random_embeddings()

    @register_embedding_function
    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __init__(self) -> None:
            pass

        @staticmethod
        def name() -> str:
            return "test"

        @staticmethod
        def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
            return TestEmbeddingFunction()

        def get_config(self) -> Dict[str, Any]:
            return {}

        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

        @staticmethod
        def validate_config(config: Dict[str, Any]) -> None:
            pass

        def validate_config_update(
            self, old_config: Dict[str, Any], new_config: Dict[str, Any]
        ) -> None:
            pass

    ef = TestEmbeddingFunction()

    embeddings = ef(random_documents())
    for i, e in enumerate(embeddings):
        assert np.array_equal(e, valid_embeddings[i])


def test_embedding_function_results_format_when_response_is_invalid() -> None:
    invalid_embedding = {"error": "test"}

    @register_embedding_function
    class TestEmbeddingFunction(EmbeddingFunction[Documents]):
        def __init__(self) -> None:
            pass

        @staticmethod
        def name() -> str:
            return "test"

        @staticmethod
        def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
            return TestEmbeddingFunction()

        def get_config(self) -> Dict[str, Any]:
            return {}

        @staticmethod
        def validate_config(config: Dict[str, Any]) -> None:
            pass

        def validate_config_update(
            self, old_config: Dict[str, Any], new_config: Dict[str, Any]
        ) -> None:
            pass

        def __call__(self, input: Documents) -> Embeddings:
            # Return something that's not a valid Embeddings type
            return cast(Embeddings, invalid_embedding)

    ef = TestEmbeddingFunction()

    # The EmbeddingFunction protocol should validate the return value
    # but we need to bypass the protocol's __call__ wrapper for this test
    with pytest.raises(ValueError):
        # This should raise a ValueError during normalization/validation
        result = ef.__call__(random_documents())
        # The normalize_embeddings function will raise a ValueError when given an invalid embedding
        from chromadb.api.types import normalize_embeddings

        normalize_embeddings(result)


def test_default_embedding_function_reuses_onnx_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DefaultEmbeddingFunction should build ONNXMiniLM_L6_V2 once per process.

    DefaultEmbeddingFunction is rebuilt from the collection configuration on
    every operation, so the ONNX session must be cached outside the instance.
    """
    from chromadb.api import types as api_types
    from chromadb.utils.embedding_functions import onnx_mini_lm_l6_v2

    constructed: List[object] = []
    valid_embeddings = random_embeddings()

    class FakeONNXMiniLM_L6_V2:
        def __init__(self) -> None:
            constructed.append(self)

        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

    monkeypatch.setattr(onnx_mini_lm_l6_v2, "ONNXMiniLM_L6_V2", FakeONNXMiniLM_L6_V2)
    monkeypatch.setattr(api_types, "_default_onnx_ef", None, raising=False)

    # A fresh DefaultEmbeddingFunction each time, as build_from_config does.
    for _ in range(3):
        api_types.DefaultEmbeddingFunction()(random_documents())

    assert len(constructed) == 1


def test_default_embedding_function_builds_once_under_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Concurrent callers on a cold cache must not each build a session.

    A server starting up serves several requests before the first embed
    finishes, which is exactly the window the cache is meant to close.
    """
    from chromadb.api import types as api_types
    from chromadb.utils.embedding_functions import onnx_mini_lm_l6_v2

    thread_count = 8
    constructed: List[object] = []
    valid_embeddings = random_embeddings()
    start = threading.Barrier(thread_count)

    class SlowFakeONNXMiniLM_L6_V2:
        def __init__(self) -> None:
            # Hold the window open so an unsynchronised check-then-set loses it.
            time.sleep(0.05)
            constructed.append(self)

        def __call__(self, input: Documents) -> Embeddings:
            return valid_embeddings

    monkeypatch.setattr(
        onnx_mini_lm_l6_v2, "ONNXMiniLM_L6_V2", SlowFakeONNXMiniLM_L6_V2
    )
    monkeypatch.setattr(api_types, "_default_onnx_ef", None, raising=False)

    def call() -> None:
        start.wait()
        api_types.DefaultEmbeddingFunction()(random_documents())

    threads = [threading.Thread(target=call) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(constructed) == 1
