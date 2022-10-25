from pydantic import BaseModel
from typing import Union, Any

# type supports single and batch mode
class AddEmbedding(BaseModel):
    embedding_data: list
    metadata: Any
    input_uri: Union[str, list]
    inference_data: Union[dict, list]
    app: Union[str, list]
    model_version: Union[str, list]
    layer: Union[str, list]
    dataset: Union[str, list] = None
    distance: Union[float, list] = None 
    category_name: Union[str, list] = None