from pydantic import BaseModel
from typing import Union, Any

class FetchEmbeddings(BaseModel):
    where_filter: object = None
    sort: str = None
    limit: int = None

# type supports single and batch mode
class AddEmbedding(BaseModel):
    embedding_data: list
    input_uri: Union[str, list]
    dataset: Union[str, list] = None
    inference_category_name: Union[str, list] = None
    label_category_name: Union[str, list] = None

class NNQueryEmbedding(BaseModel):
    query_embedding_vector: list
    n_results: int = 10
    where_filter: object = None

