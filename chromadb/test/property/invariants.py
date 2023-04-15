from chromadb.test.property.strategies import EmbeddingSet, Collection
import numpy as np
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from hypothesis import note
from hypothesis.errors import InvalidArgument


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    count = api._count(collection_name)
    assert count == expected_count


def ann_accuracy(
    collection: Collection,
    embeddings: EmbeddingSet,
    min_recall: float = 0.99,
):
    """Validate that the API performs nearest_neighbor searches correctly"""

    if len(embeddings["ids"]) == 0:
        return  # nothing to test here

    # Validate that each embedding is its own nearest neighbor and adjust recall if not.
    result = collection.query(
        query_embeddings=embeddings["embeddings"],
        query_texts=embeddings["documents"] if embeddings["embeddings"] is None else None,
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    missing = 0
    for i, id in enumerate(embeddings["ids"]):

        if result["ids"][i][0] != id:
            missing += 1
        else:
            if embeddings["embeddings"] is not None:
                assert np.allclose(result["embeddings"][i][0], embeddings["embeddings"][i])
            assert result["documents"][i][0] == (
                embeddings["documents"][i] if embeddings["documents"] is not None else None
            )
            assert result["metadatas"][i][0] == (
                embeddings["metadatas"][i] if embeddings["metadatas"] is not None else None
            )
            assert result["distances"][i][0] == 0.0

    recall = (len(embeddings["ids"]) - missing) / len(embeddings["ids"])

    try:
        note(f"recall: {recall}")
    except InvalidArgument:
        pass  # it's ok if we're running outside hypothesis

    assert recall >= min_recall
