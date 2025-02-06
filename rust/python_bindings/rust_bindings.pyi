from typing import Optional, Sequence
from uuid import UUID
from chromadb import CollectionMetadata, Embeddings, IDs
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.segment import SegmentAPI
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    Embeddings,
    IDs,
    Metadatas,
    URIs,
)
from chromadb.types import Database, Tenant, Collection as CollectionModel
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT
from enum import Enum

class Bindings:
    def __init__(
        self, proxy_frontend: SegmentAPI, sqlite_db_config: SqliteDBConfig, persist_path: str
    ) -> None: ...
    def heartbeat(self) -> int: ...
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None: ...
    def get_database(self, name: str, tenant: str = DEFAULT_TENANT) -> Database: ...
    def delete_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None: ...
    def list_databases(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        tenant: str = DEFAULT_TENANT,
    ) -> Sequence[Database]: ...
    def create_tenant(self, name: str) -> None: ...
    def get_tenant(self, name: str) -> Tenant: ...
    def create_collection(
        self,
        name: str,
        configuration: Optional[CollectionConfigurationInternal] = None,
        metadata: Optional[CollectionMetadata] = None,
        get_or_create: bool = False,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> CollectionModel: ...
    def add(
        self,
        ids: IDs,
        collection_id: str,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
        uris: Optional[URIs] = None,
        tenant: str = DEFAULT_TENANT,
        database: str = DEFAULT_DATABASE,
    ) -> bool: ...

class MigrationMode(Enum):
    Apply = 0
    Validate = 1

class MigrationHash(Enum):
    SHA256 = 0
    MD5 = 1

class SqliteDBConfig:
    url: str
    hash_type: MigrationHash
    migration_mode: MigrationMode

    def __init__(
        self, url: str, hash_type: MigrationHash, migration_mode: MigrationMode
    ) -> None: ...
