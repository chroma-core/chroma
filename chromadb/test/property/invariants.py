from typing import Sequence, cast
from chromadb.test.property.strategies import EmbeddingSet
import numpy as np
from chromadb.api import API, types
from chromadb.api.models.Collection import Collection
from hypothesis import note


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    count = api._count(collection_name)
    assert count == expected_count


def metadata_matches(collection: Collection, embeddings: EmbeddingSet):
    """The actual embedding metadata is equal to the expected metadata"""

    actual_metadata = collection.get(ids=embeddings["ids"], include=["metadatas"])[
        "metadatas"
    ]
    # TODO: read code to figure out if this can be None?
    assert actual_metadata is not None
    expected_metadata = embeddings["metadatas"]
    if expected_metadata is not None:
        cast(Sequence[types.Metadata], expected_metadata)
        for i, metadata in enumerate(actual_metadata):
            assert metadata == expected_metadata[i]


def ann_accuracy(
    collection: Collection,
    embeddings: EmbeddingSet,
    min_recall: float = 0.99,
):
    """Validate that the API performs nearest_neighbor searches correctly"""

    # Validate that each embedding is its own nearest neighbor and adjust recall if not.
    result = collection.query(
        query_embeddings=embeddings["embeddings"],
        query_texts=embeddings["documents"]
        if embeddings["embeddings"] is None
        else None,
        n_results=1,
        include=["embeddings", "documents", "metadatas", "distances"],
    )

    missing = 0
    for i, id in enumerate(embeddings["ids"]):
        if result["ids"][i][0] != id:
            missing += 1
        else:
            if embeddings["embeddings"] is not None:
                assert np.allclose(
                    result["embeddings"][i][0], embeddings["embeddings"][i]
                )
            assert result["documents"][i][0] == (
                embeddings["documents"][i]
                if embeddings["documents"] is not None
                else None
            )
            assert result["metadatas"][i][0] == (
                embeddings["metadatas"][i]
                if embeddings["metadatas"] is not None
                else None
            )
            assert result["distances"][i][0] == 0.0

    recall = (len(embeddings["ids"]) - missing) / len(embeddings["ids"])

    note(f"recall: {recall}")
    assert recall >= min_recall
