from pydantic import BaseModel
from typing import Any, Dict, Optional
from chromadb.api.types import (
    CollectionMetadata,
    Documents,
    Embeddings,
    IDs,
    Include,
    Metadatas,
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
    # TODO: Pydantic doesn't bode well with recursive types so we use generic Dicts
    # for Where and WhereDocument. This is not ideal, but it works for now since
    # there is a lot of downstream validation.
    where: Optional[Dict[Any, Any]] = None
    where_document: Optional[Dict[Any, Any]] = None
    query_embeddings: Embeddings
    n_results: int = 10
    include: Include = ["metadatas", "documents", "distances"]


class GetEmbedding(BaseModel):  # type: ignore
    ids: Optional[IDs] = None
    where: Optional[Dict[Any, Any]] = None
    where_document: Optional[Dict[Any, Any]] = None
    sort: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    include: Include = ["metadatas", "documents"]


class RawSql(BaseModel):  # type: ignore
    raw_sql: str


class DeleteEmbedding(BaseModel):  # type: ignore
    ids: Optional[IDs] = None
    where: Optional[Dict[Any, Any]] = None
    where_document: Optional[Dict[Any, Any]] = None


class CreateCollection(BaseModel):  # type: ignore
    name: str
    metadata: Optional[CollectionMetadata] = None
    get_or_create: bool = False


class UpdateCollection(BaseModel):  # type: ignore
    new_name: Optional[str] = None
    new_metadata: Optional[CollectionMetadata] = None
