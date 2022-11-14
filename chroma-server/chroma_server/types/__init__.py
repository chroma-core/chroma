from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    model_space: Union[str, list]
    embedding: list
    input_uri: Union[str, list]
    dataset: Union[str, list] = None
    category_name: Union[str, list] = None


class QueryEmbedding(BaseModel):
    model_space: str = None
    embedding: list
    n_results: int = 10
    category_name: str = None
    dataset: str = None

class ProcessEmbedding(BaseModel):
    model_space: str = None

class FetchEmbedding(BaseModel):
    where_filter: dict = {}
    sort: str = None
    limit: int = None
    offset: int = None

class CountEmbedding(BaseModel):
    model_space: str = None

class RawSql(BaseModel):
    raw_sql: str = None

class Results(BaseModel):
    model_space: str
    n_results: int = 100

class SpaceKeyInput(BaseModel):
    model_space: str

class DeleteEmbedding(BaseModel):
    where_filter: dict = {}