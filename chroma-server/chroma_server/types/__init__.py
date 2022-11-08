from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    embedding_data: list
    input_uri: Union[str, list]
    dataset: Union[str, list] = None
    custom_quality_score: Union[float, list] = None
    category_name: Union[str, list] = None


class QueryEmbedding(BaseModel):
    embedding: list
    n_results: int = 10
    category_name: str = None
    dataset: str = None
