import asyncio
from typing import Any, List, Optional, Sequence, Callable
from uuid import UUID

from overrides import override

from chromadb.api.async_api import AsyncServerAPI
from chromadb.api.collection_configuration import (
    CreateCollectionConfiguration,
    UpdateCollectionConfiguration,
)
from chromadb.api.rust import RustBindingsAPI
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    Embeddings,
    GetResult,
    IDs,
    Include,
    IndexingStatus,
    Metadatas,
    QueryResult,
    ReadLevel,
    Schema,
    SearchResult,
    URIs,
    Where,
    WhereDocument,
    IncludeMetadataDocuments,
    IncludeMetadataDocumentsDistances,
)
from chromadb.auth import UserIdentity
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.execution.expression.plan import Search
from chromadb.types import Collection as CollectionModel, Database, Tenant


class AsyncRustBindingsAPI(AsyncServerAPI):
    _sync_api: RustBindingsAPI

    def __init__(self, system: System):
        super().__init__(system)
        self._sync_api = self.require(RustBindingsAPI)

    async def _call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return await asyncio.to_thread(func, *args, **kwargs)

    @override
    async def heartbeat(self) -> int:
        return await self._call(self._sync_api.heartbeat)

    @override
    async def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        return await self._call(
            self._sync_api.count_collections, tenant=tenant, database=database
        )

    @override
    async def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[CollectionModel]:
        return await self._call(
            self._sync_api.list_collections,
            limit=limit,
            offset=offset,
            tenant=tenant,
            database=database,
        )

    @override
    async def create_collection(
        self,
        name: str,
        schema: Optional[Schema] = None,
        configuration: Optional[CreateCollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return await self._call(
            self._sync_api.create_collection,
            name=name,
            schema=schema,
            configuration=configuration,
            metadata=metadata,
            get_or_create=get_or_create,
            tenant=tenant,
            database=database,
        )

    @override
    async def get_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return await self._call(
            self._sync_api.get_collection,
            name=name,
            tenant=tenant,
            database=database,
        )

    @override
    async def get_or_create_collection(
        self,
        name: str,
        schema: Optional[Schema] = None,
        configuration: Optional[CreateCollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return await self._call(
            self._sync_api.get_or_create_collection,
            name=name,
            schema=schema,
            configuration=configuration,
            metadata=metadata,
            tenant=tenant,
            database=database,
        )

    @override
    async def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        await self._call(
            self._sync_api.delete_collection,
            name=name,
            tenant=tenant,
            database=database,
        )

    @override
    async def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
        new_configuration: Optional[UpdateCollectionConfiguration] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        await self._call(
            self._sync_api._modify,
            id=id,
            new_name=new_name,
            new_metadata=new_metadata,
            new_configuration=new_configuration,
            tenant=tenant,
            database=database,
        )

    @override
    async def _fork(
        self,
        collection_id: UUID,
        new_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return await self._call(
            self._sync_api._fork,
            collection_id=collection_id,
            new_name=new_name,
            tenant=tenant,
            database=database,
        )

    @override
    async def _get_indexing_status(
        self,
        collection_id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> IndexingStatus:
        return await self._call(
            self._sync_api._get_indexing_status,
            collection_id=collection_id,
            tenant=tenant,
            database=database,
        )

    @override
    async def _search(
        self,
        collection_id: UUID,
        searches: List[Search],
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        read_level: ReadLevel = ReadLevel.INDEX_AND_WAL,
    ) -> SearchResult:
        return await self._call(
            self._sync_api._search,
            collection_id=collection_id,
            searches=searches,
            tenant=tenant,
            database=database,
            read_level=read_level,
        )

    @override
    async def _count(
        self,
        collection_id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        return await self._call(
            self._sync_api._count,
            collection_id=collection_id,
            tenant=tenant,
            database=database,
        )

    @override
    async def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        return await self._call(
            self._sync_api._peek,
            collection_id=collection_id,
            n=n,
            tenant=tenant,
            database=database,
        )

    @override
    async def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = IncludeMetadataDocuments,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        return await self._call(
            self._sync_api._get,
            collection_id=collection_id,
            ids=ids,
            where=where,
            limit=limit,
            offset=offset,
            where_document=where_document,
            include=include,
            tenant=tenant,
            database=database,
        )

    @override
    async def _add(
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
        return await self._call(
            self._sync_api._add,
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
            tenant=tenant,
            database=database,
        )

    @override
    async def _update(
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
        return await self._call(
            self._sync_api._update,
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
            tenant=tenant,
            database=database,
        )

    @override
    async def _upsert(
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
        return await self._call(
            self._sync_api._upsert,
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
            tenant=tenant,
            database=database,
        )

    @override
    async def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        ids: Optional[IDs] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = IncludeMetadataDocumentsDistances,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> QueryResult:
        return await self._call(
            self._sync_api._query,
            collection_id=collection_id,
            query_embeddings=query_embeddings,
            ids=ids,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
            tenant=tenant,
            database=database,
        )

    @override
    async def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        await self._call(
            self._sync_api._delete,
            collection_id=collection_id,
            ids=ids,
            where=where,
            where_document=where_document,
            tenant=tenant,
            database=database,
        )

    @override
    async def reset(self) -> bool:
        return await self._call(self._sync_api.reset)

    @override
    async def get_version(self) -> str:
        return await self._call(self._sync_api.get_version)

    @override
    def get_settings(self) -> Settings:
        return self._sync_api.get_settings()

    @override
    async def get_max_batch_size(self) -> int:
        return await self._call(self._sync_api.get_max_batch_size)

    @override
    async def get_user_identity(self) -> UserIdentity:
        return await self._call(self._sync_api.get_user_identity)

    @override
    async def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        await self._call(self._sync_api.create_database, name=name, tenant=tenant)

    @override
    async def get_database(
        self, name: str, tenant: str = DEFAULT_TENANT
    ) -> Database:
        return await self._call(self._sync_api.get_database, name=name, tenant=tenant)

    @override
    async def delete_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        await self._call(self._sync_api.delete_database, name=name, tenant=tenant)

    @override
    async def list_databases(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
    ) -> Sequence[Database]:
        return await self._call(
            self._sync_api.list_databases, limit=limit, offset=offset, tenant=tenant
        )

    @override
    async def create_tenant(self, name: str) -> None:
        await self._call(self._sync_api.create_tenant, name=name)

    @override
    async def get_tenant(self, name: str) -> Tenant:
        return await self._call(self._sync_api.get_tenant, name=name)
