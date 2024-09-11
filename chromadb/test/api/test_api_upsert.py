import pytest
from chromadb.api import ClientAPI


def test_upsert_with_none_ids(client: ClientAPI) -> None:
    client.reset()
    collection = client.create_collection("test")
    with pytest.raises(ValueError) as e:
        collection.upsert(ids=None, embeddings=[[0.1, 0.2, 0.3]])  # type: ignore[arg-type]
    assert "You must provide ids when upserting." in str(e)
