from dataclasses import dataclass
from typing import Optional

from chromadb.api.types import Embeddings, IDs
from chromadb.types import RequestVersionContext, Where, WhereDocument, Collection


@dataclass
class Scan:
    collection: Collection

    @property
    def version(self) -> RequestVersionContext:
        return RequestVersionContext(
            collection_version=self.collection.version,
            log_position=self.collection.log_position,
        )


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
    uri: bool = False
