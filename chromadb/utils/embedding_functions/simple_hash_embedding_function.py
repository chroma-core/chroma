from typing import List, Dict, Any

import numpy as np

from chromadb.api.types import EmbeddingFunction, Embeddings, Documents, Space


class SimpleHashEmbeddingFunction(EmbeddingFunction[Documents]):
    """A tiny, dependency-free local embedding function used for tests and examples.

    It deterministically converts each input string into a fixed-size float32 vector
    by hashing character codes. This is intentionally simple so it works in CI
    without external model or network dependencies.
    """

    def __init__(self, dim: int = 32):
        if dim <= 0:
            raise ValueError("dim must be a positive integer")

        self.dim = int(dim)

    def _embed_one(self, text: str) -> np.ndarray:
        # Simple deterministic embedding: accumulate character ordinals into a fixed-size vector
        v = np.zeros(self.dim, dtype=np.float32)
        if not text:
            return v

        for i, ch in enumerate(text):
            idx = i % self.dim
            v[idx] += (ord(ch) % 256) / 256.0

        # Normalize to unit length to be more embedding-like
        norm = np.linalg.norm(v)
        if norm > 0:
            v = v / norm

        return v

    def __call__(self, input: Documents) -> Embeddings:
        if not input:
            return []

        return [self._embed_one(str(d)) for d in list(input)]

    def embed_query(self, input: Documents) -> Embeddings:
        # For this simple embedding, queries use the same function
        return self.__call__(input)

    @staticmethod
    def name() -> str:
        return "local_simple_hash"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        dim = config.get("dim", 32)
        return SimpleHashEmbeddingFunction(dim=dim)

    def get_config(self) -> Dict[str, Any]:
        return {"dim": self.dim}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        # Changing dim is allowed for this toy function
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        # Very small validation
        if "dim" in config:
            dim = config["dim"]
            if not isinstance(dim, int) or dim <= 0:
                raise ValueError("dim must be a positive integer")
