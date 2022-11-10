from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    space_key: Union[str, list]
    embedding_data: list
    input_uri: Union[str, list]
    dataset: Union[str, list] = None
    custom_quality_score: Union[float, list] = None
    category_name: Union[str, list] = None


class QueryEmbedding(BaseModel):
    space_key: str = None
    embedding: list
    n_results: int = 10
    category_name: str = None
    dataset: str = None

class ProcessEmbedding(BaseModel):
    space_key: str = None

class FetchEmbedding(BaseModel):
    where_filter: dict = {}
    sort: str = None
    limit: int = None

class CountEmbedding(BaseModel):
    space_key: str = None

class RawSql(BaseModel):
    raw_sql: str = None