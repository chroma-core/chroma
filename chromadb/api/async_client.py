import httpx
from typing import Optional, Sequence
from uuid import UUID
from overrides import override
from chromadb.api import AsyncAdminAPI, AsyncClientAPI, AsyncServerAPI
from chromadb.api.configuration import CollectionConfiguration
from chromadb.api.models.AsyncCollection import AsyncCollection
from chromadb.api.shared_system_client import SharedSystemClient
from chromadb.api.types import (
    CollectionMetadata,
    DataLoader,
    Documents,
    Embeddable,
    EmbeddingFunction,
    Embeddings,
    GetResult,
    IDs,
    Include,
    Loadable,
    Metadatas,
    QueryResult,
    URIs,
)
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System
from chromadb.errors import ChromaError
from chromadb.types import Database, Tenant, Where, WhereDocument
import chromadb.utils.embedding_functions as ef


class AsyncClient(SharedSystemClient, AsyncClientAPI):
    """A client for Chroma. This is the main entrypoint for interacting with Chroma.
    A client internally stores its tenant and database and proxies calls to a
    Server API instance of Chroma. It treats the Server API and corresponding System
    as a singleton, so multiple clients connecting to the same resource will share the
    same API instance.

    Client implementations should be implement their own API-caching strategies.
    """

    # An internal admin client for verifying that databases and tenants exist
    _admin_client: AsyncAdminAPI

    tenant: str = DEFAULT_TENANT
    database: str = DEFAULT_DATABASE

    _server: AsyncServerAPI

    @classmethod
    async def create(
        cls,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        settings: Settings = Settings(),
    ) -> "AsyncClient":
        # Create an admin client for verifying that databases and tenants exist
        self = cls(settings=settings)
        SharedSystemClient._populate_data_from_system(self._system)
        self._admin_client = AsyncAdminClient.from_system(self._system)
        await self._validate_tenant_database(tenant=tenant, database=database)

        self.tenant = tenant
        self.database = database

        # Get the root system component we want to interact with
        self._server = self._system.instance(AsyncServerAPI)

        self._submit_client_start_event()

        return self

    @classmethod
    # (we can't override and use from_system() because it's synchronous)
    async def from_system_async(
        cls,
        system: System,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> "AsyncClient":
        """Create a client from an existing system. This is useful for testing and debugging."""
        return await AsyncClient.create(tenant, database, system.settings)

    @classmethod
    @override
    def from_system(
        cls,
        system: System,
    ) -> "SharedSystemClient":
        """AsyncClient cannot be created synchronously. Use .from_system_async() instead."""
        raise NotImplementedError(
            "AsyncClient cannot be created synchronously. Use .from_system_async() instead."
        )

    @override
    async def set_tenant(self, tenant: str, database: str = DEFAULT_DATABASE) -> None:
        await self._validate_tenant_database(tenant=tenant, database=database)
        self.tenant = tenant
        self.database = database

    @override
    async def set_database(self, database: str) -> None:
        await self._validate_tenant_database(tenant=self.tenant, database=database)
        self.database = database

    async def _validate_tenant_database(self, tenant: str, database: str) -> None:
        try:
            await self._admin_client.get_tenant(name=tenant)
        except httpx.ConnectError:
            raise ValueError(
                "Could not connect to a Chroma server. Are you sure it is running?"
            )
        # Propagate ChromaErrors
        except ChromaError as e:
            raise e
        except Exception:
            raise ValueError(
                f"Could not connect to tenant {tenant}. Are you sure it exists?"
            )

        try:
            await self._admin_client.get_database(name=database, tenant=tenant)
        except httpx.ConnectError:
            raise ValueError(
                "Could not connect to a Chroma server. Are you sure it is running?"
            )

    # region BaseAPI Methods
    # Note - we could do this in less verbose ways, but they break type checking
    @override
    async def heartbeat(self) -> int:
        return await self._server.heartbeat()

    @override
    async def list_collections(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> Sequence[AsyncCollection]:
        models = await self._server.list_collections(
            limit, offset, tenant=self.tenant, database=self.database
        )
        return [
            AsyncCollection(
                client=self._server,
                model=model,
            )
            for model in models
        ]

    @override
    async def count_collections(self) -> int:
        return await self._server.count_collections(
            tenant=self.tenant, database=self.database
        )

    @override
    async def create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        get_or_create: bool = False,
    ) -> AsyncCollection:
        model = await self._server.create_collection(
            name=name,
            configuration=configuration,
            metadata=metadata,
            tenant=self.tenant,
            database=self.database,
            get_or_create=get_or_create,
        )
        return AsyncCollection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    async def get_collection(
        self,
        name: str,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
    ) -> AsyncCollection:
        model = await self._server.get_collection(
            id=id,
            name=name,
            tenant=self.tenant,
            database=self.database,
        )
        return AsyncCollection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    async def get_or_create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
    ) -> AsyncCollection:
        model = await self._server.get_or_create_collection(
            name=name,
            configuration=configuration,
            metadata=metadata,
            tenant=self.tenant,
            database=self.database,
        )
        return AsyncCollection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    async def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        return await self._server._modify(
            id=id,
            new_name=new_name,
            new_metadata=new_metadata,
        )

    @override
    async def delete_collection(
        self,
        name: str,
    ) -> None:
        return await self._server.delete_collection(
            name=name,
            tenant=self.tenant,
            database=self.database,
        )

    #
    # ITEM METHODS
    #

    @override
    async def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        return await self._server._add(
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
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
    ) -> bool:
        return await self._server._update(
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
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
    ) -> bool:
        return await self._server._upsert(
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        )

    @override
    async def _count(self, collection_id: UUID) -> int:
        return await self._server._count(
            collection_id=collection_id,
        )

    @override
    async def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        return await self._server._peek(
            collection_id=collection_id,
            n=n,
        )

    @override
    async def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["embeddings", "metadatas", "documents"],  # type: ignore[list-item]
    ) -> GetResult:
        return await self._server._get(
            collection_id=collection_id,
            ids=ids,
            where=where,
            sort=sort,
            limit=limit,
            offset=offset,
            page=page,
            page_size=page_size,
            where_document=where_document,
            include=include,
        )

    async def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs],
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> None:
        await self._server._delete(
            collection_id=collection_id,
            ids=ids,
            where=where,
            where_document=where_document,
        )

    @override
    async def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["embeddings", "metadatas", "documents", "distances"],  # type: ignore[list-item]
    ) -> QueryResult:
        return await self._server._query(
            collection_id=collection_id,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

    @override
    async def reset(self) -> bool:
        return await self._server.reset()

    @override
    async def get_version(self) -> str:
        return await self._server.get_version()

    @override
    def get_settings(self) -> Settings:
        return self._server.get_settings()

    @override
    async def get_max_batch_size(self) -> int:
        return await self._server.get_max_batch_size()

    @override
    async def close(self) -> None:
        await self._server.close()
        await self._admin_client.close()

    # endregion


class AsyncAdminClient(SharedSystemClient, AsyncAdminAPI):
    _server: AsyncServerAPI

    def __init__(self, settings: Settings = Settings()) -> None:
        super().__init__(settings)
        self._server = self._system.instance(AsyncServerAPI)

    @override
    async def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        return await self._server.create_database(name=name, tenant=tenant)

    @override
    async def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        return await self._server.get_database(name=name, tenant=tenant)

    @override
    async def create_tenant(self, name: str) -> None:
        return await self._server.create_tenant(name=name)

    @override
    async def get_tenant(self, name: str) -> Tenant:
        return await self._server.get_tenant(name=name)

    @classmethod
    @override
    def from_system(
        cls,
        system: System,
    ) -> "AsyncAdminClient":
        SharedSystemClient._populate_data_from_system(system)
        instance = cls(settings=system.settings)
        return instance

    @override
    async def close(self) -> None:
        await self._server.close()
