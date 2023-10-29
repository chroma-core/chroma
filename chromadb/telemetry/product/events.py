from typing import cast, ClassVar
from chromadb.telemetry.product import ProductTelemetryEvent
from chromadb.utils.embedding_functions import get_builtins


class ClientStartEvent(ProductTelemetryEvent):
    def __init__(self) -> None:
        super().__init__()


class ClientCreateCollectionEvent(ProductTelemetryEvent):
    collection_uuid: str
    embedding_function: str

    def __init__(self, collection_uuid: str, embedding_function: str):
        super().__init__()
        self.collection_uuid = collection_uuid

        embedding_function_names = get_builtins()

        self.embedding_function = (
            embedding_function
            if embedding_function in embedding_function_names
            else "custom"
        )


class CollectionAddEvent(ProductTelemetryEvent):
    max_batch_size: ClassVar[int] = 100
    batch_size: int
    collection_uuid: str
    add_amount: int
    with_documents: int
    with_metadata: int

    def __init__(
        self,
        collection_uuid: str,
        add_amount: int,
        with_documents: int,
        with_metadata: int,
        batch_size: int = 1,
    ):
        super().__init__()
        self.collection_uuid = collection_uuid
        self.add_amount = add_amount
        self.with_documents = with_documents
        self.with_metadata = with_metadata
        self.batch_size = batch_size

    @property
    def batch_key(self) -> str:
        return self.collection_uuid + self.name

    def batch(self, other: "ProductTelemetryEvent") -> "CollectionAddEvent":
        if not self.batch_key == other.batch_key:
            raise ValueError("Cannot batch events")
        other = cast(CollectionAddEvent, other)
        total_amount = self.add_amount + other.add_amount
        return CollectionAddEvent(
            collection_uuid=self.collection_uuid,
            add_amount=total_amount,
            with_documents=self.with_documents + other.with_documents,
            with_metadata=self.with_metadata + other.with_metadata,
            batch_size=self.batch_size + other.batch_size,
        )


class CollectionUpdateEvent(ProductTelemetryEvent):
    collection_uuid: str
    update_amount: int
    with_embeddings: int
    with_metadata: int
    with_documents: int

    def __init__(
        self,
        collection_uuid: str,
        update_amount: int,
        with_embeddings: int,
        with_metadata: int,
        with_documents: int,
    ):
        super().__init__()
        self.collection_uuid = collection_uuid
        self.update_amount = update_amount
        self.with_embeddings = with_embeddings
        self.with_metadata = with_metadata
        self.with_documents = with_documents


class CollectionQueryEvent(ProductTelemetryEvent):
    max_batch_size: ClassVar[int] = 20
    batch_size: int
    collection_uuid: str
    query_amount: int
    with_metadata_filter: int
    with_document_filter: int
    n_results: int
    include_metadatas: int
    include_documents: int
    include_distances: int

    def __init__(
        self,
        collection_uuid: str,
        query_amount: int,
        with_metadata_filter: int,
        with_document_filter: int,
        n_results: int,
        include_metadatas: int,
        include_documents: int,
        include_distances: int,
        batch_size: int = 1,
    ):
        super().__init__()
        self.collection_uuid = collection_uuid
        self.query_amount = query_amount
        self.with_metadata_filter = with_metadata_filter
        self.with_document_filter = with_document_filter
        self.n_results = n_results
        self.include_metadatas = include_metadatas
        self.include_documents = include_documents
        self.include_distances = include_distances
        self.batch_size = batch_size

    @property
    def batch_key(self) -> str:
        return self.collection_uuid + self.name

    def batch(self, other: "ProductTelemetryEvent") -> "CollectionQueryEvent":
        if not self.batch_key == other.batch_key:
            raise ValueError("Cannot batch events")
        other = cast(CollectionQueryEvent, other)
        total_amount = self.query_amount + other.query_amount
        return CollectionQueryEvent(
            collection_uuid=self.collection_uuid,
            query_amount=total_amount,
            with_metadata_filter=self.with_metadata_filter + other.with_metadata_filter,
            with_document_filter=self.with_document_filter + other.with_document_filter,
            n_results=self.n_results + other.n_results,
            include_metadatas=self.include_metadatas + other.include_metadatas,
            include_documents=self.include_documents + other.include_documents,
            include_distances=self.include_distances + other.include_distances,
            batch_size=self.batch_size + other.batch_size,
        )


class CollectionGetEvent(ProductTelemetryEvent):
    collection_uuid: str
    ids_count: int
    limit: int
    include_metadata: int
    include_documents: int

    def __init__(
        self,
        collection_uuid: str,
        ids_count: int,
        limit: int,
        include_metadata: int,
        include_documents: int,
    ):
        super().__init__()
        self.collection_uuid = collection_uuid
        self.ids_count = ids_count
        self.limit = limit
        self.include_metadata = include_metadata
        self.include_documents = include_documents


class CollectionDeleteEvent(ProductTelemetryEvent):
    collection_uuid: str
    delete_amount: int

    def __init__(self, collection_uuid: str, delete_amount: int):
        super().__init__()
        self.collection_uuid = collection_uuid
        self.delete_amount = delete_amount
