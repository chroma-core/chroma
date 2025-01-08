from chromadb.api import ClientAPI
import numpy as np

from chromadb.errors import InvalidDimensionException


def test_duplicate_collection_create(
    client: ClientAPI,
) -> None:
    _ = client.create_collection(
        name="test",
        metadata={"hnsw:construction_ef": 128, "hnsw:search_ef": 128, "hnsw:M": 128},
    )

    try:
        client.create_collection(
            name="test",
            metadata={
                "hnsw:construction_ef": 128,
                "hnsw:search_ef": 128,
                "hnsw:M": 128,
            },
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection creation failed as expected with error ", e)
        assert "already exists" in e.args[0] or "UniqueConstraintError" in e.args[0]


def test_not_existing_collection_delete(
    client: ClientAPI,
) -> None:
    try:
        _ = client.delete_collection(
            name="test101",
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection deletion failed as expected with error ", e)
        assert "does not exist" in e.args[0]


def test_collection_dimension_mismatch(
    client: ClientAPI,
) -> None:
    collection = client.create_collection(
        name="test",
    )
    D = 768
    N = 5
    embeddings = np.random.random(size=(N, D))
    ids = [str(i) for i in range(N)]

    collection.add(ids=ids, embeddings=embeddings)  # type: ignore[arg-type]

    WRONG_D = 512
    wrong_embeddings = np.random.random(size=(N, WRONG_D))
    try:
        collection.add(ids=ids, embeddings=wrong_embeddings)  # type: ignore[arg-type]
        assert False, "Expected exception"
    except InvalidDimensionException:
        print("Dimension mismatch failed as expected")
    except Exception as e:
        assert False, f"Unexpected exception {e}"
