"""local_simple_hash_example.py

A tiny, dependency-free example showing how to use the `local_simple_hash`
embedding function that ships with the repository. This is useful for quick
smoke tests in CI or on machines without model downloads or API keys.

Usage:
    # from the repository root, in an activated venv
    python -m pip install -e .
    python examples/local_simple_hash_example.py

The script demonstrates:
- constructing the embedding function directly
- using config_to_embedding_function to build from a config dict
- embedding a few example inputs including non-strings

This script is intentionally small and dependency-free.
"""

from typing import Sequence

import numpy as np

from chromadb.utils.embedding_functions.simple_hash_embedding_function import (
    SimpleHashEmbeddingFunction,
)
from chromadb.utils.embedding_functions import config_to_embedding_function


def print_embedding_info(emb: np.ndarray) -> None:
    """Print concise information about a single embedding vector."""
    print(
        f"len={emb.shape[0]}, dtype={emb.dtype}, norm={float(np.linalg.norm(emb)):.6f}"
    )


def main() -> None:
    # Option A: construct directly
    ef = SimpleHashEmbeddingFunction(dim=16)

    # Convert inputs to strings to satisfy the embedding function's expected input type
    raw_docs: Sequence[object] = [
        "The quick brown fox jumps over the lazy dog",
        "ChromaDB local embedding example",
        "",  # empty string -> zero vector
        12345,  # non-string input will be stringified
    ]

    docs: list[str] = [str(d) for d in raw_docs]
    embeddings = ef(docs)

    print("Embeddings from SimpleHashEmbeddingFunction:")
    for i, e in enumerate(embeddings):
        print(f"doc {i}:", end=" ")
        print_embedding_info(e)

    # Option B: build from config (useful when embedding functions are configured by
    # JSON/YAML). This demonstrates `config_to_embedding_function` integration.
    cfg = {"name": "local_simple_hash", "config": {"dim": 16}}
    ef2 = config_to_embedding_function(cfg)
    embs2 = ef2(["hello from cfg"])
    print("\nEmbeddings from config-built function:")
    print_embedding_info(embs2[0])


if __name__ == "__main__":
    main()
