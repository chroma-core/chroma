from pydantic import BaseModel
from typing import List, Union
from chromadb.api.types import Include


# type supports single and batch mode
class AddEmbedding(BaseModel):
    embeddings: List
    metadatas: Union[List, dict] = None
    documents: Union[str, List] = None
    ids: Union[str, List] = None
    increment_index: bool = True


class UpdateEmbedding(BaseModel):
    embeddings: List = None
    metadatas: Union[List, dict] = None
    documents: Union[str, List] = None
    ids: Union[str, List] = None
    increment_index: bool = True


class QueryEmbedding(BaseModel):
    where: dict = {}
    where_document: dict = {}
    query_embeddings: List
    n_results: int = 10
    include: Include = ["metadatas", "documents", "distances"]


class ProcessEmbedding(BaseModel):
    collection_name: str = None
    training_dataset_name: str = None
    unlabeled_dataset_name: str = None


class GetEmbedding(BaseModel):
    ids: List = None
    where: dict = None
    where_document: dict = None
    sort: str = None
    limit: int = None
    offset: int = None
    include: Include = ["metadatas", "documents"]


class CountEmbedding(BaseModel):
    collection_name: str = None


class RawSql(BaseModel):
    raw_sql: str = None


class SpaceKeyInput(BaseModel):
    collection_name: str


class DeleteEmbedding(BaseModel):
    ids: List = None
    where: dict = None
    where_document: dict = None


class CreateCollection(BaseModel):
    name: str
    metadata: dict = None
    get_or_create: bool = False


class UpdateCollection(BaseModel):
    new_name: str = None
    new_metadata: dict = None
