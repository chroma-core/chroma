from dataclasses import dataclass
from typing import ClassVar
from chromadb.telemetry import TelemetryEvent


@dataclass
class ClientStartEvent(TelemetryEvent):
    name: ClassVar[str] = "client_start"


@dataclass
class ServerStartEvent(TelemetryEvent):
    name: ClassVar[str] = "server_start"


@dataclass
class ClientCreateCollectionEvent(TelemetryEvent):
    name: ClassVar[str] = "client_create_collection"
    collection_uuid: str
    embedding_function: str


@dataclass
class CollectionAddEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_add"
    collection_uuid: str
    add_amount: int
    with_embeddings: bool
    with_metadatas: bool
    with_documents: bool


@dataclass
class CollectionUpdateEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_update"
    collection_uuid: str
    update_amount: int
    with_embeddings: bool
    with_metadatas: bool
    with_documents: bool


@dataclass
class CollectionQueryEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_query"
    collection_uuid: str
    with_metadata_filter: bool
    with_document_filter: bool
    query_with_embeddings: bool
    query_size: int
    n_neighbors: int
    including: str


@dataclass
class CollectionGetEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_get"
    collection_uuid: str
    with_ids: bool
    with_metadata_filter: bool
    with_document_filter: bool
    including: str


@dataclass
class CollectionDeleteEvent(TelemetryEvent):
    name: ClassVar[str] = "collection_delete"
    collection_uuid: str
    delete_amount: int
