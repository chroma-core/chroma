from uuid import UUID, uuid4

from chromadb.api.models.Collection import Collection
from chromadb.types import Collection as CollectionModel


class FlushClient:
    def __init__(self) -> None:
        self.flushed_collection_ids: list[UUID] = []

    def _flush(self, collection_id: UUID) -> None:
        self.flushed_collection_ids.append(collection_id)


def test_collection_flush_routes_to_internal_api() -> None:
    collection_id = uuid4()
    client = FlushClient()
    collection = Collection(
        client=client,  # type: ignore[arg-type]
        model=CollectionModel(
            id=collection_id,
            name="flush-test",
            configuration_json={},
            serialized_schema=None,
            metadata=None,
            dimension=None,
            tenant="default_tenant",
            database="default_database",
        ),
        embedding_function=None,
    )

    collection.flush()

    assert client.flushed_collection_ids == [collection_id]
