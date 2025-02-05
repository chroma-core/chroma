from typing import Optional, Sequence
from chromadb import CollectionMetadata
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.segment import SegmentAPI
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT
from chromadb.types import Collection, Database, Tenant
from enum import Enum

class Bindings:
    def __init__(
        self, proxy_frontend: SegmentAPI, sqlite_db_config: SqliteDBConfig
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
    ) -> Collection: ...

###################### rust/sqlite  ######################
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
