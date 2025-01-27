from typing import Optional, Sequence
from chromadb import CollectionMetadata
from chromadb.api.configuration import CollectionConfigurationInternal
from chromadb.api.segment import SegmentAPI
from chromadb.config import DEFAULT_DATABASE, DEFAULT_TENANT
from chromadb.types import Collection, Database, Tenant

class Bindings:
    def __init__(self, proxy_frontend: SegmentAPI) -> None: ...
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
