from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    model_space: Union[str, list]
    embedding: list
    input_uri: Union[str, list]
    dataset: Union[str, list] = None
    inference_class: Union[str, list] = None
    label_class: Union[str, list] = None

class QueryEmbedding(BaseModel):
    where: dict = {}
    embedding: list
    n_results: int = 10

class ProcessEmbedding(BaseModel):
    model_space: str = None
    training_dataset_name: str = None
    unlabeled_dataset_name: str = None

class FetchEmbedding(BaseModel):
    where: dict = {}
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
    dataset_name: str = None

class SpaceKeyInput(BaseModel):
    model_space: str

class DeleteEmbedding(BaseModel):
    where: dict = {}
