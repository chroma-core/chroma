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
from chromadb.api.collection_configuration import (
    CreateCollectionConfiguration,
    UpdateCollectionConfiguration,
    create_collection_configuration_to_json_str,
    update_collection_configuration_to_json_str,
)
from chromadb.auth import UserIdentity
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT, Settings, System

from chromadb.api.types import (
    IncludeMetadataDocuments,
    IncludeMetadataDocumentsDistances,
    IncludeMetadataDocumentsEmbeddings,
)

# TODO(hammadb): Unify imports across types vs root __init__.py
from chromadb.types import Database, Tenant, Collection as CollectionModel
import chromadb_rust_bindings


from typing import Optional, Sequence
from overrides import override
from uuid import UUID
import json
import platform
from pathlib import Path
import uuid
import os

if platform.system() != "Windows":
    import resource
elif platform.system() == "Windows":
    import ctypes

USER_ID_PATH = str(Path.home() / ".cache" / "chroma" / "telemetry_user_id")
UNKNOWN_USER_ID = "UNKNOWN"


# RustBindingsAPI is an implementation of ServerAPI which shims
# the Rust bindings to the Python API, providing a full implementation
# of the API. It could be that bindings was a direct implementation of
# ServerAPI, but in order to prevent propagating the bindings types
# into the Python API, we have to shim it here so we can convert into
# the legacy Python types.
# TODO(hammadb): Propagate the types from the bindings into the Python API
# and remove the python-level types entirely.
class RustBindingsAPI(ServerAPI):
    bindings: chromadb_rust_bindings.Bindings
    hnsw_cache_size: int

    def __init__(self, system: System):
        super().__init__(system)

        if platform.system() != "Windows":
            max_file_handles = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
        else:
            max_file_handles = ctypes.windll.msvcrt._getmaxstdio()  # type: ignore
        self.hnsw_cache_size = (
            max_file_handles
            # This is integer division in Python 3, and not a comment.
            # Each HNSW index has 4 data files and 1 metadata file
            // 5
        )

    @override
    def start(self) -> None:
        # Set the anonymized_telemetry environment variable for Rust
        anonymized_telemetry = self._system.settings.anonymized_telemetry
        os.environ["ANONYMIZED_TELEMETRY"] = "1" if anonymized_telemetry else "0"

        # Construct the SqliteConfig
        # TOOD: We should add a "config converter"
        if self._system.settings.require("is_persistent"):
            persist_path = self._system.settings.require("persist_directory")
            sqlite_persist_path = persist_path + "/chroma.sqlite3"
        else:
            persist_path = None
            sqlite_persist_path = None
        hash_type = self._system.settings.require("migrations_hash_algorithm")
        hash_type_bindings = (
            chromadb_rust_bindings.MigrationHash.MD5
            if hash_type == "md5"
            else chromadb_rust_bindings.MigrationHash.SHA256
        )
        migration_mode = self._system.settings.require("migrations")
        migration_mode_bindings = (
            chromadb_rust_bindings.MigrationMode.Apply
            if migration_mode == "apply"
            else chromadb_rust_bindings.MigrationMode.Validate
        )
        sqlite_config = chromadb_rust_bindings.SqliteDBConfig(
            hash_type=hash_type_bindings,
            migration_mode=migration_mode_bindings,
            url=sqlite_persist_path,
        )

        # Determine telemetry user ID (moved from __init__ attempt)
        user_id_path = USER_ID_PATH
        try:
            if os.path.exists(user_id_path):
                with open(user_id_path, "r") as f:
                    telemetry_user_id = f.read()
            else:
                # Ensure the directory exists before writing
                os.makedirs(os.path.dirname(user_id_path), exist_ok=True)
                telemetry_user_id = str(uuid.uuid4())
                with open(user_id_path, "w") as f:
                    f.write(telemetry_user_id)
        except Exception:
            telemetry_user_id = UNKNOWN_USER_ID

        self.bindings = chromadb_rust_bindings.Bindings(
            self._system.settings.require("allow_reset"),
            sqlite_config,
            self.hnsw_cache_size,
            telemetry_user_id,
            persist_path,
            self._system.settings.anonymized_telemetry,
        )

    @override
    def stop(self) -> None:
        del self.bindings

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
        tenant = self.bindings.get_tenant(name)
        return Tenant(name=tenant.name)

    # ////////////////////////////// Base API //////////////////////////////

    @override
    def heartbeat(self) -> int:
        return self.bindings.heartbeat()

    @override
    def count_collections(
        self, tenant: str = DEFAULT_TENANT, database: str = DEFAULT_DATABASE
    ) -> int:
        return self.bindings.count_collections(tenant, database)

    @override
    def list_collections(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> Sequence[CollectionModel]:
        collections = self.bindings.list_collections(limit, offset, tenant, database)
        return [
            CollectionModel(
                id=collection.id,
                name=collection.name,
                configuration_json=collection.configuration,
                metadata=collection.metadata,
                dimension=collection.dimension,
                tenant=collection.tenant,
                database=collection.database,
            )
            for collection in collections
        ]

    @override
    def create_collection(
        self,
        name: str,
        configuration: Optional[CreateCollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        if configuration:
            configuration_json_str = create_collection_configuration_to_json_str(
                configuration
            )
        else:
            configuration_json_str = None

        collection = self.bindings.create_collection(
            name, configuration_json_str, metadata, get_or_create, tenant, database
        )
        collection_model = CollectionModel(
            id=collection.id,
            name=collection.name,
            configuration_json=collection.configuration,
            metadata=collection.metadata,
            dimension=collection.dimension,
            tenant=collection.tenant,
            database=collection.database,
        )
        return collection_model

    @override
    def get_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        collection = self.bindings.get_collection(name, tenant, database)
        return CollectionModel(
            id=collection.id,
            name=collection.name,
            configuration_json=collection.configuration,
            metadata=collection.metadata,
            dimension=collection.dimension,
            tenant=collection.tenant,
            database=collection.database,
        )

    @override
    def get_or_create_collection(
        self,
        name: str,
        configuration: Optional[CreateCollectionConfiguration] = None,
        metadata: Optional[CollectionMetadata] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        return self.create_collection(
            name, configuration, metadata, True, tenant, database
        )

    @override
    def delete_collection(
        self,
        name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        self.bindings.delete_collection(name, tenant, database)

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
        new_configuration: Optional[UpdateCollectionConfiguration] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> None:
        if new_configuration:
            new_configuration_json_str = update_collection_configuration_to_json_str(
                new_configuration
            )
        else:
            new_configuration_json_str = None
        self.bindings.update_collection(
            str(id), new_name, new_metadata, new_configuration_json_str
        )

    @override
    def _fork(
        self,
        collection_id: UUID,
        new_name: str,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel:
        raise NotImplementedError(
            "Collection forking is not implemented for Local Chroma"
        )

    @override
    def _count(
        self,
        collection_id: UUID,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> int:
        return self.bindings.count(str(collection_id), tenant, database)

    @override
    def _peek(
        self,
        collection_id: UUID,
        n: int = 10,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> GetResult:
        return self._get(
            collection_id,
            limit=n,
            tenant=tenant,
            database=database,
            include=IncludeMetadataDocumentsEmbeddings,
        )

    @override
    def _get(
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

        return GetResult(
            ids=rust_response.ids,
            embeddings=rust_response.embeddings,
            documents=rust_response.documents,
            uris=rust_response.uris,
            included=include,
            data=None,
            metadatas=rust_response.metadatas,
        )

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
        return self.bindings.update(
            str(collection_id),
            ids,
            embeddings,
            metadatas,
            documents,
            uris,
            tenant,
            database,
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
        return self.bindings.upsert(
            str(collection_id),
            ids,
            embeddings,
            metadatas,
            documents,
            uris,
            tenant,
            database,
        )

    @override
    def _query(
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
        rust_response = self.bindings.query(
            str(collection_id),
            ids,
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
        return self.bindings.delete(
            str(collection_id),
            ids,
            json.dumps(where) if where else None,
            json.dumps(where_document) if where_document else None,
            tenant,
            database,
        )

    @override
    def reset(self) -> bool:
        return self.bindings.reset()

    @override
    def get_version(self) -> str:
        return self.bindings.get_version()

    @override
    def get_settings(self) -> Settings:
        return self._system.settings

    @override
    def get_max_batch_size(self) -> int:
        return self.bindings.get_max_batch_size()

    # TODO: Remove this if it's not planned to be used
    @override
    def get_user_identity(self) -> UserIdentity:
        return UserIdentity(
            user_id="",
            tenant=DEFAULT_TENANT,
            databases=[DEFAULT_DATABASE],
        )
