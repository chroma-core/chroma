from chromadb.api.types import EmbeddingFunction, Embeddable, Embeddings
import numpy as np
from typing import cast, Any


class LegacyCustomEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([1, 2, 3]).tolist())


class CustomEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __call__(self, input: Embeddable) -> Embeddings:
        return cast(Embeddings, np.array([1, 2, 3]).tolist())

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    @staticmethod
    def name() -> str:
        return "custom_embedding_function"

    @staticmethod
    def build_from_config(config: dict[str, Any]) -> "CustomEmbeddingFunction":
        return CustomEmbeddingFunction()

    def get_config(self) -> dict[str, Any]:
        return {}


def test_legacy_custom_ef() -> None:
    ef = LegacyCustomEmbeddingFunction()
    result = ef(["test"])

    # Check the structure: we expect a list with one NumPy array
    assert isinstance(result, list), "Result should be a list"
    assert len(result) == 1, "Result should contain exactly one element"
    assert isinstance(result[0], np.ndarray), "Result element should be a NumPy array"

    # Compare the contents of the array
    expected = np.array([1, 2, 3], dtype=np.float32)
    assert np.array_equal(
        result[0], expected
    ), f"Arrays not equal: {result[0]} vs {expected}"


def test_custom_ef() -> None:
    ef = CustomEmbeddingFunction()
    result = ef(["test"])

    # Same checks as above
    assert isinstance(result, list), "Result should be a list"
    assert len(result) == 1, "Result should contain exactly one element"
    assert isinstance(result[0], np.ndarray), "Result element should be a NumPy array"

    expected = np.array([1, 2, 3], dtype=np.float32)
    assert np.array_equal(
        result[0], expected
    ), f"Arrays not equal: {result[0]} vs {expected}"
