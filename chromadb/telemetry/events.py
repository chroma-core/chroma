from dataclasses import dataclass
from typing import cast, ClassVar
from chromadb.telemetry import TelemetryEvent


@dataclass
class ClientStartEvent(TelemetryEvent):
    name: ClassVar[str] = "client_start"
    max_batch_size = 1


@dataclass
class ServerStartEvent(TelemetryEvent):
    name: ClassVar[str] = "server_start"
    max_batch_size = 1


@dataclass
class ClientCreateCollectionEvent(TelemetryEvent):
    name: ClassVar[str] = "client_create_collection"
    collection_uuid: str
    embedding_function: str
    max_batch_size = 1


@dataclass
class CollectionAddEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_add"
    collection_uuid: str
    add_amount: int
    with_embeddings: bool
    with_metadata: bool
    with_documents: bool
    max_batch_size = 4

    def can_batch(self, other: TelemetryEvent) -> bool:
        return (
            isinstance(other, CollectionAddEvent)
            and self.collection_uuid == other.collection_uuid
            and self.with_embeddings == other.with_embeddings
            and self.with_metadata == other.with_metadata
            and self.with_documents == other.with_documents
        )

    def batch(self, other: "TelemetryEvent") -> "CollectionAddEvent":
        if not self.can_batch(other):
            raise ValueError("Cannot batch events")
        other = cast(CollectionAddEvent, other)
        return CollectionAddEvent(
            collection_uuid=self.collection_uuid,
            add_amount=self.add_amount + other.add_amount,
            with_embeddings=self.with_embeddings,
            with_metadata=self.with_metadata,
            with_documents=self.with_documents,
        )


@dataclass
class CollectionUpdateEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_update"
    collection_uuid: str
    update_amount: int
    with_embeddings: bool
    with_metadata: bool
    with_documents: bool
    max_batch_size = 1


@dataclass
class CollectionQueryEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_query"
    collection_uuid: str
    query_amount: int
    with_metadata_filter: bool
    with_document_filter: bool
    n_results: int
    include_metadatas: bool
    include_documents: bool
    include_distances: bool
    max_batch_size = 1


@dataclass
class CollectionGetEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_get"
    collection_uuid: str
    ids_count: int
    limit: int
    include_metadata: bool
    include_documents: bool
    max_batch_size = 1


@dataclass
class CollectionDeleteEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_delete"
    collection_uuid: str
    delete_amount: int
    max_batch_size = 1
