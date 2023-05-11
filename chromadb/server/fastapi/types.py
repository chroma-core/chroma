from pydantic import BaseModel
from typing import Optional
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    Embeddings,
    IDs,
    Include,
    Metadatas,
    Where,
    WhereDocument,
)


class AddEmbedding(BaseModel):  # type: ignore
    embeddings: Optional[Embeddings] = None
    metadatas: Optional[Metadatas] = None
    documents: Optional[Documents] = None
    ids: IDs
    increment_index: bool = True


class UpdateEmbedding(BaseModel):  # type: ignore
    embeddings: Optional[Embeddings] = None
    metadatas: Optional[Metadatas] = None
    documents: Optional[Documents] = None
    ids: IDs
    increment_index: bool = True


class QueryEmbedding(BaseModel):  # type: ignore
    where: Where = {}
    where_document: WhereDocument = {}
    query_embeddings: Embeddings
    n_results: int = 10
    include: Include = ["metadatas", "documents", "distances"]


class GetEmbedding(BaseModel):  # type: ignore
    ids: Optional[IDs] = None
    where: Optional[Where] = None
    where_document: Optional[WhereDocument] = None
    sort: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    include: Include = ["metadatas", "documents"]


class RawSql(BaseModel):  # type: ignore
    raw_sql: str


class DeleteEmbedding(BaseModel):  # type: ignore
    ids: Optional[IDs] = None
    where: Optional[Where] = None
    where_document: Optional[WhereDocument] = None


class CreateCollection(BaseModel):  # type: ignore
    name: str
    metadata: Optional[CollectionMetadata] = None
    get_or_create: bool = False


class UpdateCollection(BaseModel):  # type: ignore
    new_name: Optional[str] = None
    new_metadata: Optional[CollectionMetadata] = None
