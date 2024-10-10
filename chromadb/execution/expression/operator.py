from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from chromadb.types import RequestVersionContext, Where, WhereDocument
from chromadb.api.types import Embeddings, IDs


@dataclass
class Scan:
    collection: UUID
    metadata: UUID
    record: UUID
    vector: UUID
    version: RequestVersionContext


@dataclass
class Filter:
    user_ids: Optional[IDs] = None
    where: Optional[Where] = None
    where_document: Optional[WhereDocument] = None


@dataclass
class KNN:
    embeddings: Embeddings
    fetch: int


@dataclass
class Limit:
    skip: int = 0
    fetch: Optional[int] = None


@dataclass
class Projection:
    document: bool = False
    embedding: bool = False
    metadata: bool = False
    rank: bool = False
