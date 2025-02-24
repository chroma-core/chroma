import logging
from typing import List, cast, Union

import httpx

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class JinaEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the Jina AI API.
    It requires an API key and a model name. The default model name is "jina-embeddings-v3".
    """

    def __init__(self, api_key: str, model_name: str = "jina-embeddings-v3", task: str = "text-matching", late_chunking: bool = False, dimensions: int = 1024, embedding_type: str = "float"):
        """
        Initialize the JinaEmbeddingFunction.

        Args:
            api_key (str): Your API key for the Jina AI API.
            model_name (str, optional): The name of the model to use for text embeddings. Defaults to "jina-embeddings-v3".
            task (str,  optional): The model will generate optimized embeddings for that task. Defaults to "text-matching".
            late_chunking (bool,  optional): The model will generate contextual chunk embeddings. Defaults to "False"
            dimensions (int,  optional): Number of dimensions. Defaults to "1024".
            embedding_type (str, optional): Type of embedding to be returned. Defaults to "float".
        """
        self._model_name = model_name
        self._task = task
        self._late_chunking = late_chunking
        self._dimensions = dimensions
        self._embedding_type = embedding_type
        self._api_url = "https://api.jina.ai/v1/embeddings"
        self._session = httpx.Client()
        self._session.headers.update(
            {"Authorization": f"Bearer {api_key}", "Accept-Encoding": "identity"}
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            texts (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> jina_ai_fn = JinaEmbeddingFunction(api_key="your_api_key")
            >>> input = ["Hello, world!", "How are you?"]
            >>> embeddings = jina_ai_fn(input)
        """
        # Call Jina AI Embedding API
        resp = self._session.post(
            self._api_url, json={
                "input": input, 
                "model": self._model_name, 
                "task": self._task, 
                "late_chunking": self._late_chunking,
                "dimensions": self._dimensions, 
                "embedding_type":self._embedding_type
            }
        ).json()
        if "data" not in resp:
            raise RuntimeError(resp["detail"])

        embeddings: List[dict[str, Union[str, List[float]]]] = resp["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings, key=lambda e: e["index"])

        # Return just the embeddings
        return cast(Embeddings, [result["embedding"] for result in sorted_embeddings])
