import hashlib
from typing import Dict, Any, List, Optional

import numpy as np

from chromadb.api.types import (
    Documents,
    Embeddings,
    EmbeddingFunction,
)
from chromadb.utils.embedding_functions import register_embedding_function


@register_embedding_function
class MockEmbeddingFunction(EmbeddingFunction[Documents]):
    """A deterministic embedding function for testing that requires no external dependencies.

    Generates embeddings by hashing input text with SHA-256, producing reproducible
    vectors of configurable dimension and dtype. Identical inputs always produce
    identical embeddings.

    Args:
        dim: The dimension of generated embeddings. Defaults to 256.
        dtype: The numpy dtype of generated embeddings. Defaults to np.float32.
        seed: Optional seed prepended to input text before hashing, allowing
              different MockEmbeddingFunction instances to produce different
              embeddings for the same input.
    """

    def __init__(
        self,
        dim: int = 256,
        dtype: np.dtype = np.float32,
        seed: Optional[str] = None,
    ) -> None:
        self._dim = dim
        self._dtype = dtype
        self._seed = seed

    def __call__(self, input: Documents) -> Embeddings:
        embeddings: Embeddings = []
        for text in input:
            hash_input = text if self._seed is None else f"{self._seed}{text}"
            hash_bytes = hashlib.sha256(hash_input.encode("utf-8")).digest()
            # Extend hash bytes to fill the desired dimension
            extended = hash_bytes
            while len(extended) < self._dim * 4:
                extended += hashlib.sha256(extended).digest()
            values = [
                int.from_bytes(extended[i * 4 : (i + 1) * 4], "little") / (2**32)
                for i in range(self._dim)
            ]
            embeddings.append(np.array(values, dtype=self._dtype))
        return embeddings

    @staticmethod
    def name() -> str:
        return "mock"

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "dim": self._dim,
            "dtype": str(self._dtype),
        }
        if self._seed is not None:
            config["seed"] = self._seed
        return config

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "MockEmbeddingFunction":
        return MockEmbeddingFunction(
            dim=config.get("dim", 256),
            dtype=np.dtype(config.get("dtype", "float32")),
            seed=config.get("seed"),
        )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        if "dim" in config and (not isinstance(config["dim"], int) or config["dim"] <= 0):
            raise ValueError(f"dim must be a positive integer, got {config['dim']}")

    def default_space(self) -> str:  # type: ignore[override]
        return "cosine"
