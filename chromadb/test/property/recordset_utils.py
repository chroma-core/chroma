import uuid
from random import randint
from typing import Sequence, cast

from chromadb.api.types import Embeddings

import chromadb.test.property.strategies as strategies


def create_large_recordset(
    min_size: int = 45000,
    max_size: int = 50000,
    *,
    embedding: Sequence = (1, 2, 3),
) -> strategies.RecordSet:
    """Build a large record set without relying on Hypothesis generation."""
    size = randint(min_size, max_size)
    ids = [str(uuid.uuid4()) for _ in range(size)]
    metadatas = [{"some_key": f"{i}"} for i in range(size)]
    documents = [f"Document {i}" for i in range(size)]
    embeddings = [list(embedding) for _ in range(size)]
    return strategies.RecordSet(
        ids=ids,
        embeddings=cast(Embeddings, embeddings),
        metadatas=metadatas,
        documents=documents,
    )
