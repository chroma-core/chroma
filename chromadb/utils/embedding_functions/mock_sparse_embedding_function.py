import hashlib
from typing import Dict, Any, List, Optional

from chromadb.api.types import (
    Documents,
    SparseEmbeddingFunction,
)
from chromadb.base_types import SparseVector, SparseVectors
from chromadb.utils.embedding_functions import register_sparse_embedding_function


@register_sparse_embedding_function
class MockSparseEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    """A deterministic sparse embedding function for testing that requires no external dependencies.

    Generates sparse vectors by hashing input text with SHA-256. Each input
    produces a reproducible sparse vector with a configurable number of
    non-zero entries and vocabulary size.

    Args:
        vocab_size: The size of the vocabulary (max index value). Defaults to 30000.
        nnz: The number of non-zero entries per vector. Defaults to 20.
        seed: Optional seed prepended to input text before hashing.
    """

    def __init__(
        self,
        vocab_size: int = 30000,
        nnz: int = 20,
        seed: Optional[str] = None,
    ) -> None:
        self._vocab_size = vocab_size
        self._nnz = nnz
        self._seed = seed

    def __call__(self, input: Documents) -> SparseVectors:
        vectors: SparseVectors = []
        for text in input:
            hash_input = text if self._seed is None else f"{self._seed}{text}"
            hash_bytes = hashlib.sha256(hash_input.encode("utf-8")).digest()
            extended = hash_bytes
            while len(extended) < self._nnz * 8:
                extended += hashlib.sha256(extended).digest()

            indices: List[int] = []
            values: List[float] = []
            for i in range(self._nnz):
                idx_bytes = extended[i * 4 : (i + 1) * 4]
                val_bytes = extended[self._nnz * 4 + i * 4 : self._nnz * 4 + (i + 1) * 4]
                idx = int.from_bytes(idx_bytes, "little") % self._vocab_size
                val = int.from_bytes(val_bytes, "little") / (2**32)
                indices.append(idx)
                values.append(val)

            # Deduplicate indices, keeping the first occurrence
            seen: Dict[int, float] = {}
            for idx, val in zip(indices, values):
                if idx not in seen:
                    seen[idx] = val

            sorted_items = sorted(seen.items())
            vectors.append(
                SparseVector(
                    indices=[item[0] for item in sorted_items],
                    values=[item[1] for item in sorted_items],
                )
            )
        return vectors

    @staticmethod
    def name() -> str:
        return "mock_sparse"

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "vocab_size": self._vocab_size,
            "nnz": self._nnz,
        }
        if self._seed is not None:
            config["seed"] = self._seed
        return config

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "MockSparseEmbeddingFunction":
        return MockSparseEmbeddingFunction(
            vocab_size=config.get("vocab_size", 30000),
            nnz=config.get("nnz", 20),
            seed=config.get("seed"),
        )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        if "vocab_size" in config and (
            not isinstance(config["vocab_size"], int) or config["vocab_size"] <= 0
        ):
            raise ValueError(
                f"vocab_size must be a positive integer, got {config['vocab_size']}"
            )
        if "nnz" in config and (
            not isinstance(config["nnz"], int) or config["nnz"] <= 0
        ):
            raise ValueError(f"nnz must be a positive integer, got {config['nnz']}")
