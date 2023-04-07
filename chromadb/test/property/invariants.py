import numpy as np
from chromadb.test.property.strategies import EmbeddingSet
from chromadb.api import API
from chromadb.api.models.Collection import Collection


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    count = api._count(collection_name)
    assert count == expected_count


def ann_accuracy(
    collection: Collection,
    embeddings: EmbeddingSet,
):
    """Validate that the API performs nearest_neighbor searches correctly"""

    # Validate that each embedding is its own nearest neighbor
    result = collection.query(
        query_embeddings=embeddings["embeddings"],
        query_texts=embeddings["documents"] if embeddings["embeddings"] is None else None,
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    for i, id in enumerate(embeddings["ids"]):
        assert result["ids"][i][0] == id
        if embeddings["embeddings"] is not None:
            assert np.allclose(result["embeddings"][i][0], embeddings["embeddings"][i])
        assert result["documents"][i][0] == (
            embeddings["documents"][i] if embeddings["documents"] is not None else None
        )
        assert result["metadatas"][i][0] == (
            embeddings["metadatas"][i] if embeddings["metadatas"] is not None else {}
        )
        assert result["distances"][i][0] == 0.0
