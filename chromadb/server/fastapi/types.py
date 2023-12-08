from pydantic import BaseModel
from typing import Any, Dict, List, Optional
from chromadb.api.types import (
    CollectionMetadata,
    Include,
)


class AddEmbedding(BaseModel):
    # Pydantic doesn't handle Union types cleanly like Embeddings which has
    # Union[int, float] so we use Any here to ensure data is parsed
    # to its original type.
    embeddings: Optional[List[Any]] = None
    metadatas: Optional[List[Optional[Dict[Any, Any]]]] = None
    documents: Optional[List[Optional[str]]] = None
    uris: Optional[List[Optional[str]]] = None
    ids: List[str]


class UpdateEmbedding(BaseModel):
    embeddings: Optional[List[Any]] = None
    metadatas: Optional[List[Optional[Dict[Any, Any]]]] = None
    documents: Optional[List[Optional[str]]] = None
    uris: Optional[List[Optional[str]]] = None
    ids: List[str]


class QueryEmbedding(BaseModel):
    # TODO: Pydantic doesn't bode well with recursive types so we use generic Dicts
    # for Where and WhereDocument. This is not ideal, but it works for now since
    # there is a lot of downstream validation.
    where: Optional[Dict[Any, Any]] = {}
    where_document: Optional[Dict[Any, Any]] = {}
    query_embeddings: List[Any]
    n_results: int = 10
    include: Include = ["metadatas", "documents", "distances"]


class GetEmbedding(BaseModel):
    ids: Optional[List[str]] = None
    where: Optional[Dict[Any, Any]] = None
    where_document: Optional[Dict[Any, Any]] = None
    sort: Optional[str] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    include: Include = ["metadatas", "documents"]


class DeleteEmbedding(BaseModel):
    ids: Optional[List[str]] = None
    where: Optional[Dict[Any, Any]] = None
    where_document: Optional[Dict[Any, Any]] = None


class CreateCollection(BaseModel):
    name: str
    metadata: Optional[CollectionMetadata] = None
    get_or_create: bool = False


class UpdateCollection(BaseModel):
    new_name: Optional[str] = None
    new_metadata: Optional[CollectionMetadata] = None


class CreateDatabase(BaseModel):
    name: str


class CreateTenant(BaseModel):
    name: str
