from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    embeddings: list
    metadatas: Union[list, dict] = None
    documents: Union[str, list] = None
    ids: Union[str, list] = None
    increment_index: bool = True


class UpdateEmbedding(BaseModel):
    embeddings: list = None
    metadatas: Union[list, dict] = None
    documents: Union[str, list] = None
    ids: Union[str, list] = None
    increment_index: bool = True


class QueryEmbedding(BaseModel):
    where: dict = {}
    where_document: dict = {}
    query_embeddings: list
    n_results: int = 10


class ProcessEmbedding(BaseModel):
    collection_name: str = None
    training_dataset_name: str = None
    unlabeled_dataset_name: str = None


class GetEmbedding(BaseModel):
    ids: list = None
    where: dict = None
    where_document: dict = None
    sort: str = None
    limit: int = None
    offset: int = None


class CountEmbedding(BaseModel):
    collection_name: str = None


class RawSql(BaseModel):
    raw_sql: str = None


class SpaceKeyInput(BaseModel):
    collection_name: str


class DeleteEmbedding(BaseModel):
    ids: list = None
    where: dict = None
    where_document: dict = None


class CreateCollection(BaseModel):
    name: str
    metadata: dict = None


class UpdateCollection(BaseModel):
    new_name: str = None
    new_metadata: dict = None
