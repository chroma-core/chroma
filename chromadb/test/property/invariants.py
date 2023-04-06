from chromadb.test.property.strategies import EmbeddingSet
from chromadb.api import API


def count(api: API, collection_name: str, expected_count: int):
    """The given collection count is equal to the number of embeddings"""
    count = api._count(collection_name)
    assert count == expected_count


def ann_accuracy(
    api: API,
    collection_name: str,
    embeddings: EmbeddingSet,
    precision: float = 0.9,
    recall: float = 0.9,
):
    """Validate that the API performs nearest_neighbor searches with the expected
    precision and recall"""
    # TODO: do in-process brute-force as comparison
    pass
