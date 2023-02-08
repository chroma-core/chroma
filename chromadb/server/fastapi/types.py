from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    collection_name: Union[str, list]
    embedding: list
    metadata: Union[str, list] = None

class QueryEmbedding(BaseModel):
    where: dict = {}
    embedding: list
    n_results: int = 10

class ProcessEmbedding(BaseModel):
    collection_name: str = None
    training_dataset_name: str = None
    unlabeled_dataset_name: str = None

class FetchEmbedding(BaseModel):
    where: dict = {}
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
    where: dict = {}

class CreateCollection(BaseModel):
    name: str
    metadata: dict = None

class UpdateCollection(BaseModel):
    metadata: dict = None
