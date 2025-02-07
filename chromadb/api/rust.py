from chromadb import (
    CollectionMetadata,
    Embeddings,
    GetResult,
    IDs,
    Where,
    WhereDocument,
    Include,
    Documents,
    Metadatas,
    QueryResult,
    URIs,
)
from chromadb.api import ServerAPI
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.segment import SegmentAPI
from chromadb.auth import UserIdentity
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System

# TODO(hammadb): Unify imports across types vs root __init__.py
from chromadb.types import Database, Tenant, Collection as CollectionModel
import rust_bindings

from typing import Optional, Sequence
from overrides import override
from uuid import UUID
import platform

if platform.system() != "Windows":
    import resource
elif platform.system() == "Windows":
    import ctypes
import json


# RustBindingsAPI is an implementation of ServerAPI which shims
# the Rust bindings to the Python API, providing a full implementation
# of the API. It could be that bindings was a direct implementation of
# ServerAPI, but in order to prevent propagating the bindings types
# into the Python API, we have to shim it here so we can convert into
# the legacy Python types.
# TODO(hammadb): Propagate the types from the bindings into the Python API
# and remove the python-level types entirely.
class RustBindingsAPI(ServerAPI):
    bindings: rust_bindings.Bindings
    # NOTE(hammadb) We proxy all calls to this instance of the Segment API
    proxy_segment_api: SegmentAPI

    def __init__(self, system: System):
        super().__init__(system)
        self.proxy_segment_api = system.require(SegmentAPI)

        # Construct the SqliteConfig
        # TOOD: We should add a "config converter"
        persist_path = system.settings.require("persist_directory")
        # TODO: How to name this file?
        # TODO: proper path handling
        sqlite_persist_path = persist_path
        hash_type = system.settings.require("migrations_hash_algorithm")
        hash_type_bindings = (
            rust_bindings.MigrationHash.MD5
            if hash_type == "md5"
            else rust_bindings.MigrationHash.SHA256
        )
        migration_mode = system.settings.require("migrations")
        migration_mode_bindings = (
            rust_bindings.MigrationMode.Apply
            if migration_mode == "apply"
            else rust_bindings.MigrationMode.Validate
        )
        sqlite_config = rust_bindings.SqliteDBConfig(
            url=sqlite_persist_path,
            hash_type=hash_type_bindings,
            migration_mode=migration_mode_bindings,
        )
        if platform.system() != "Windows":
            max_file_handles = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        else:
            max_file_handles = ctypes.windll.msvcrt._getmaxstdio()  # type: ignore
        hnsw_cache_size = (
            max_file_handles
            # This is integer division in Python 3, and not a comment.
            # Each HNSW index has 4 data files and 1 metadata file
            // 5
        )

        # Construct the Rust bindings
        self.bindings = rust_bindings.Bindings(
            sqlite_db_config=sqlite_config,
            persist_path=persist_path,
            hnsw_cache_size=hnsw_cache_size,
        )

    # ////////////////////////////// Admin API //////////////////////////////

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        return self.bindings.create_database(name, tenant)

    @override
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database:
        database = self.bindings.get_database(name, tenant)
        return {
            "id": database.id,
            "name": database.name,
            "tenant": database.tenant,
        }

    @override
    def delete_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        return self.bindings.delete_database(name, tenant)

    @override
    def list_databases(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
    ) -> Sequence[Database]:
        databases = self.bindings.list_databases(limit, offset, tenant)
        return [
            {
                "id": database.id,
                "name": database.name,
                "tenant": database.tenant,
            }
            for database in databases
        ]

    @override
    def create_tenant(self, name: str) -> None:
        return self.bindings.create_tenant(name)

    @override
    def get_tenant(self, name: str) -> Tenant:
        return self.bindings.get_tenant(name)

    # ////////////////////////////// Base API //////////////////////////////

    @override
    def heartbeat(self) -> int:
        return self.bindings.heartbeat()

    @override
    def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        return self.proxy_segment_api.count_collections(tenant, database)

    @override
    def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[CollectionModel]:
        return self.proxy_segment_api.list_collections(limit, offset, tenant, database)

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
        collection = self.bindings.create_collection(
            name, configuration, metadata, get_or_create, tenant, database
        )
        collection = CollectionModel(
            id=collection.id,
            name=collection.name,
            configuration=collection.configuration,  # type: ignore
            metadata=collection.metadata,
            dimension=collection.dimension,
            tenant=collection.tenant,
            database=collection.database,
        )

        return collection

    @override
    def get_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return self.proxy_segment_api.get_collection(name, tenant, database)

    @override
    def get_or_create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfigurationInternal] = None,
        metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return self.proxy_segment_api.get_or_create_collection(
            name, configuration, metadata, tenant, database
        )

    @override
    def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        return self.proxy_segment_api.delete_collection(name, tenant, database)

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        return self.proxy_segment_api._modify(
            id, new_name, new_metadata, tenant, database
        )

    @override
    def _count(
        self,
        collection_id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        return self.proxy_segment_api._count(collection_id, tenant, database)  # type: ignore[no-any-return]

    @override
    def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        return self.proxy_segment_api._peek(collection_id, n, tenant, database)

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
        rust_response = self.bindings.get(
            str(collection_id),
            ids,
            json.dumps(where) if where else None,
            limit,
            offset or 0,
            json.dumps(where_document) if where_document else None,
            include,
            tenant,
            database,
        )
        # TODO: The data field is missing from rust?
        return GetResult(
            ids=rust_response.ids,
            embeddings=rust_response.embeddings,
            documents=rust_response.documents,
            uris=rust_response.uris,
            included=include,
            data=None,
            metadatas=rust_response.metadatas,
        )

        # return self.proxy_segment_api._get(  # type: ignore[no-any-return]
        #     collection_id,
        #     ids,
        #     where,
        #     sort,
        #     limit,
        #     offset,
        #     page,
        #     page_size,
        #     where_document,
        #     include,
        #     tenant,
        #     database,
        # )

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
        return self.bindings.add(
            ids,
            str(collection_id),
            embeddings,
            metadatas,
            documents,
            uris,
            tenant,
            database,
        )
        # return self.proxy_segment_api._add(
        #     ids, collection_id, embeddings, metadatas, documents, uris, tenant, database
        # )

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
        # return self.bindings.update(
        #     str(collection_id),
        #     ids,
        #     embeddings,
        #     metadatas,
        #     documents,
        #     uris,
        #     tenant,
        #     database,
        # )
        return self.proxy_segment_api._update(
            collection_id, ids, embeddings, metadatas, documents, uris, tenant, database
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
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> bool:
        return self.proxy_segment_api._upsert(
            collection_id, ids, embeddings, metadatas, documents, uris, tenant, database
        )

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
        rust_response = self.bindings.query(
            str(collection_id),
            query_embeddings,
            n_results,
            json.dumps(where) if where else None,
            json.dumps(where_document) if where_document else None,
            include,
            tenant,
            database,
        )

        return QueryResult(
            ids=rust_response.ids,
            embeddings=rust_response.embeddings,
            documents=rust_response.documents,
            uris=rust_response.uris,
            included=include,
            data=None,
            metadatas=rust_response.metadatas,
            distances=rust_response.distances,
        )

        # return self.proxy_segment_api._query(  # type: ignore[no-any-return]
        #     collection_id,
        #     query_embeddings,
        #     n_results,
        #     where,
        #     where_document,
        #     include,
        #     tenant,
        #     database,
        # )

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
        return self.proxy_segment_api._delete(
            collection_id, ids, where, where_document, tenant, database
        )

    @override
    def reset(self) -> bool:
        return self.proxy_segment_api.reset()

    @override
    def get_version(self) -> str:
        return self.proxy_segment_api.get_version()

    @override
    def get_settings(self) -> Settings:
        return self.proxy_segment_api.get_settings()

    @override
    def get_max_batch_size(self) -> int:
        max_size = self.bindings.get_max_batch_size()
        return max_size

    @override
    def get_user_identity(self) -> UserIdentity:
        return self.proxy_segment_api.get_user_identity()
