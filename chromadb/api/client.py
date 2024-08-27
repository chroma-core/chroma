from typing import Optional, Sequence
from uuid import UUID

from overrides import override
import httpx
from chromadb.api import AdminAPI, ClientAPI, ServerAPI
from chromadb.api.configuration import CollectionConfiguration
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
    AddResult,
    URIs,
)
from chromadb.config import Settings, System
from chromadb.config import DEFAULT_TENANT, DEFAULT_DATABASE
from chromadb.api.models.Collection import Collection
from chromadb.errors import ChromaError
from chromadb.types import Database, Tenant, Where, WhereDocument
import chromadb.utils.embedding_functions as ef


class Client(SharedSystemClient, ClientAPI):
    """A client for Chroma. This is the main entrypoint for interacting with Chroma.
    A client internally stores its tenant and database and proxies calls to a
    Server API instance of Chroma. It treats the Server API and corresponding System
    as a singleton, so multiple clients connecting to the same resource will share the
    same API instance.

    Client implementations should be implement their own API-caching strategies.
    """

    tenant: str = DEFAULT_TENANT
    database: str = DEFAULT_DATABASE

    _server: ServerAPI
    # An internal admin client for verifying that databases and tenants exist
    _admin_client: AdminAPI

    # region Initialization
    def __init__(
        self,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
        settings: Settings = Settings(),
    ) -> None:
        super().__init__(settings=settings)
        self.tenant = tenant
        self.database = database
        # Create an admin client for verifying that databases and tenants exist
        self._admin_client = AdminClient.from_system(self._system)
        self._validate_tenant_database(tenant=tenant, database=database)

        # Get the root system component we want to interact with
        self._server = self._system.instance(ServerAPI)

        self._submit_client_start_event()

    @classmethod
    @override
    def from_system(
        cls,
        system: System,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> "Client":
        SharedSystemClient._populate_data_from_system(system)
        instance = cls(tenant=tenant, database=database, settings=system.settings)
        return instance

    # endregion

    # region BaseAPI Methods
    # Note - we could do this in less verbose ways, but they break type checking
    @override
    def heartbeat(self) -> int:
        return self._server.heartbeat()

    @override
    def list_collections(
        self, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> Sequence[Collection]:
        return [
            Collection(client=self._server, model=model)
            for model in self._server.list_collections(
                limit, offset, tenant=self.tenant, database=self.database
            )
        ]

    @override
    def count_collections(self) -> int:
        return self._server.count_collections(
            tenant=self.tenant, database=self.database
        )

    @override
    def create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
        get_or_create: bool = False,
    ) -> Collection:
        model = self._server.create_collection(
            name=name,
            metadata=metadata,
            tenant=self.tenant,
            database=self.database,
            get_or_create=get_or_create,
            configuration=configuration,
        )
        return Collection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    def get_collection(
        self,
        name: str,
        id: Optional[UUID] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
    ) -> Collection:
        model = self._server.get_collection(
            id=id,
            name=name,
            tenant=self.tenant,
            database=self.database,
        )
        return Collection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    def get_or_create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[
            EmbeddingFunction[Embeddable]
        ] = ef.DefaultEmbeddingFunction(),  # type: ignore
        data_loader: Optional[DataLoader[Loadable]] = None,
    ) -> Collection:
        model = self._server.get_or_create_collection(
            name=name,
            metadata=metadata,
            tenant=self.tenant,
            database=self.database,
            configuration=configuration,
        )
        return Collection(
            client=self._server,
            model=model,
            embedding_function=embedding_function,
            data_loader=data_loader,
        )

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        return self._server._modify(
            id=id,
            new_name=new_name,
            new_metadata=new_metadata,
        )

    @override
    def delete_collection(
        self,
        name: str,
    ) -> None:
        return self._server.delete_collection(
            name=name,
            tenant=self.tenant,
            database=self.database,
        )

    #
    # ITEM METHODS
    #

    @override
    def _add(
        self,
        collection_id: UUID,
        embeddings: Embeddings,
        ids: Optional[IDs] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> AddResult:
        return self._server._add(
            ids=ids,
            collection_id=collection_id,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        )

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        return self._server._update(
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        )

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
    ) -> bool:
        return self._server._upsert(
            collection_id=collection_id,
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            uris=uris,
        )

    @override
    def _count(self, collection_id: UUID) -> int:
        return self._server._count(
            collection_id=collection_id,
        )

    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        return self._server._peek(
            collection_id=collection_id,
            n=n,
        )

    @override
    def _get(
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
        return self._server._get(
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

    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs],
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        return self._server._delete(
            collection_id=collection_id,
            ids=ids,
            where=where,
            where_document=where_document,
        )

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Where = {},
        where_document: WhereDocument = {},
        include: Include = ["embeddings", "metadatas", "documents", "distances"],  # type: ignore[list-item]
    ) -> QueryResult:
        return self._server._query(
            collection_id=collection_id,
            query_embeddings=query_embeddings,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

    @override
    def reset(self) -> bool:
        return self._server.reset()

    @override
    def get_version(self) -> str:
        return self._server.get_version()

    @override
    def get_settings(self) -> Settings:
        return self._server.get_settings()

    @override
    def get_max_batch_size(self) -> int:
        return self._server.get_max_batch_size()

    # endregion

    # region ClientAPI Methods

    @override
    def set_tenant(self, tenant: str, database: str = DEFAULT_DATABASE) -> None:
        self._validate_tenant_database(tenant=tenant, database=database)
        self.tenant = tenant
        self.database = database

    @override
    def set_database(self, database: str) -> None:
        self._validate_tenant_database(tenant=self.tenant, database=database)
        self.database = database

    def _validate_tenant_database(self, tenant: str, database: str) -> None:
        try:
            self._admin_client.get_tenant(name=tenant)
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
            self._admin_client.get_database(name=database, tenant=tenant)
        except httpx.ConnectError:
            raise ValueError(
                "Could not connect to a Chroma server. Are you sure it is running?"
            )
        except Exception:
            raise ValueError(
                f"Could not connect to database {database} for tenant {tenant}. Are you sure it exists?"
            )

    # endregion


class AdminClient(SharedSystemClient, AdminAPI):
    _server: ServerAPI

    def __init__(self, settings: Settings = Settings()) -> None:
        super().__init__(settings)
        self._server = self._system.instance(ServerAPI)

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        return self._server.create_database(name=name, tenant=tenant)

    @override
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        return self._server.get_database(name=name, tenant=tenant)

    @override
    def create_tenant(self, name: str) -> None:
        return self._server.create_tenant(name=name)

    @override
    def get_tenant(self, name: str) -> Tenant:
        return self._server.get_tenant(name=name)

    @classmethod
    @override
    def from_system(
        cls,
        system: System,
    ) -> "AdminClient":
        SharedSystemClient._populate_data_from_system(system)
        instance = cls(settings=system.settings)
        return instance
