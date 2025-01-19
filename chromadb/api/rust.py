from typing import Optional, Sequence
from uuid import UUID
from chromadb import (
    CollectionMetadata,
    Documents,
    Embeddings,
    GetResult,
    IDs,
    QueryResult,
    Where,
    WhereDocument,
    Include,
    Metadatas,
    URIs,
)
from chromadb.api import ServerAPI
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.auth import UserIdentity
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from overrides import override
from rust_bindings import Bindings
from chromadb.types import Database, Tenant, Collection as CollectionModel


class RustBindingsAPI(ServerAPI):
    def __init__(self, system: System):
        super().__init__(system)
        self.bindings = Bindings()

    @override
    def heartbeat(self) -> int:
        # TODO: add pyi file for types
        return self.bindings.heartbeat()  # type: ignore

    # ////////////////////////////// Admin API //////////////////////////////

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        self.bindings.create_database(name, tenant)

    @override
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        # TODO: type
        return self.bindings.get_database(name, tenant)  # type: ignore

    @override
    def delete_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        raise NotImplementedError()

    @override
    def list_databases(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
    ) -> Sequence[Database]:
        raise NotImplementedError()

    @override
    def create_tenant(self, name: str) -> None:
        # TODO: type
        self.bindings.create_tenant(name)

    @override
    def get_tenant(self, name: str) -> Tenant:
        # TODO: type
        return self.bindings.get_tenant(name)  # type: ignore

    # ////////////////////////////// Base API //////////////////////////////

    @override
    def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        raise NotImplementedError()

    @override
    def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[CollectionModel]:
        raise NotImplementedError()

    @override
    def create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfigurationInternal] = None,
        metadata: Optional[CollectionMetadata] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        raise NotImplementedError()

    @override
    def get_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        raise NotImplementedError()

    @override
    def get_or_create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfigurationInternal] = None,
        metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        raise NotImplementedError()

    @override
    def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        raise NotImplementedError()

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        raise NotImplementedError()

    @override
    def _count(
        self,
        collection_id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        raise NotImplementedError()

    @override
    def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        raise NotImplementedError()

    @override
    def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],  # type: ignore[list-item]
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        raise NotImplementedError()

    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> bool:
        raise NotImplementedError()

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents", "distances"],  # type: ignore[list-item]
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> QueryResult:
        raise NotImplementedError()

    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        raise NotImplementedError()

    @override
    def reset(self) -> bool:
        raise NotImplementedError()

    @override
    def get_version(self) -> str:
        raise NotImplementedError()

    @override
    def get_settings(self) -> Settings:
        raise NotImplementedError()

    @override
    def get_max_batch_size(self) -> int:
        """Return the maximum number of records that can be created or mutated in a single call."""
        raise NotImplementedError()

    @override
    def get_user_identity(self) -> UserIdentity:
        return self.bindings.get_user_identity()  # type: ignore
