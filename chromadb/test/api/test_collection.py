from chromadb.api import ClientAPI
from chromadb.errors import UniqueConstraintError


def test_duplicate_collection_create(
    client: ClientAPI,
) -> None:
    collection = client.create_collection(
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
        assert "already exists" in e.args[0] or isinstance(e, UniqueConstraintError)


def test_not_existing_collection_delete(
    client: ClientAPI,
) -> None:
    try:
        collection = client.delete_collection(
            name="test101",
        )
        assert False, "Expected exception"
    except Exception as e:
        print("Collection deletion failed as expected with error ", e)
        assert "does not exist" in e.args[0]
