import pytest
from typing import Callable, List
from chromadb.api import ClientAPI
from chromadb.api.types import IncludeEnum
from chromadb.api.models.Collection import Collection


@pytest.fixture
def setup_chroma_collection(client: ClientAPI) -> Collection:
    collection_name = "test_max_distance"
    try:
        client.delete_collection(name=collection_name)
    except Exception as e:
        print(f"Collection '{collection_name}' does not exist or failed to delete: {e}")

    collection = client.create_collection(name=collection_name)
    documents = [
        "This is a document about pineapple.",
        "Oranges are a type of citrus fruit.",
        "Who is the president of the United States in 2025?",
        "Hawaii is known for its beautiful beaches and culture.",
        "A quantum computer can solve complex problems much faster.",
        "The history of Rome includes the rise and fall of the Roman Empire.",
        "Machine learning models are trained on large datasets.",
        "Chocolate is loved by millions across the world.",
        "Deep sea exploration uncovers new species in the ocean.",
        "The theory of relativity revolutionized modern physics.",
    ]

    collection.add(
        documents=documents,
        ids=[f"id{i}" for i in range(1, len(documents) + 1)],
    )
    return collection


@pytest.mark.parametrize(
    "query_text, n_results, max_distance, distance_check",
    [
        (
            "What is Hawaii known for?",  # Query text
            5,  # n_results
            None,  # max_distance (no filter)
            lambda distances: all(
                distance > 0 for distance in distances
            ),  # distance_check
        ),
        (
            "What is Hawaii known for?",
            10,
            1.5,
            lambda distances: all(distance <= 1.5 for distance in distances),
        ),
        (
            "What is Hawaii known for?",
            10,
            5.0,
            lambda distances: all(distance <= 5.0 for distance in distances),
        ),
        (
            "What is Hawaii known for?",
            10,
            0.5,
            lambda distances: all(distance <= 0.5 for distance in distances),
        ),
        (
            "What is Hawaii known for?",
            10,
            0.0,
            lambda distances: len(distances) == 0,
        ),
    ],
)
def test_query_with_different_max_distances(
    setup_chroma_collection: Collection,
    query_text: str,
    n_results: int,
    max_distance: float,
    distance_check: Callable[[List[float]], bool],
) -> None:
    collection = setup_chroma_collection

    results = collection.query(
        query_texts=[query_text],
        n_results=n_results,
        include=[IncludeEnum.distances, IncludeEnum.documents],
        max_distance=max_distance,
    )

    distances = results["distances"]
    assert distances is not None, "Distances should not be None."
    assert "documents" in results, "Documents should be returned."
    assert "distances" in results, "Distances should be returned."
    assert distance_check(distances[0]), "Distances do not meet the expected condition."


@pytest.mark.xfail
def test_query_with_invalid_max_distance_type(
    setup_chroma_collection: Collection,
) -> None:
    collection = setup_chroma_collection

    with pytest.raises(TypeError, match="Expected max_distance to be a float"):
        collection.query(
            query_texts="Who is the president of United States?",
            n_results=2,
            include=[IncludeEnum.distances, IncludeEnum.documents],
            max_distance=1,
        )
