from dataclasses import dataclass
from typing import Optional
from uuid import UUID

from chromadb.types import Where, WhereDocument
from chromadb.api.types import Embeddings, IDs


@dataclass
class Scan:
    segment: UUID
    collection: UUID


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
class Project:
    document: bool = False
    embedding: bool = False
    metadata: bool = False
