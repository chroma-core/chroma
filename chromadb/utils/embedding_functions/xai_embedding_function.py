import logging
from typing import List, cast, TypedDict

import httpx

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings, Embedding

logger = logging.getLogger(__name__)

class XAIEmbedding(TypedDict):
    Float: List[float]

class XAIResponseItem(TypedDict):
    embedding: Embedding
    index: int
    object: str


class XAIEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the XAI API.
    It requires an API key and a model name. You can use the "list embedding models" endpoint
    to verify what embeddings models are available for your API key.
    """

    def __init__(self, api_key: str, model_name: str):
        """
        Initialize the XAIEmbeddingFunction.

        Args:
            api_key (str): Your API key for the XAI API.
            model_name (str, optional): The name of the model to use for embeddings.
        """
        self._model_name = model_name
        self._api_url = "https://api.x.ai/v1/embeddings"
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
        """
        resp = self._session.post(
            self._api_url, json={"input": input, "model": self._model_name, "encoding_format": "float"}
        ).json()
        if "data" not in resp:
            raise RuntimeError(resp["error"])

        embeddings: List[XAIResponseItem] = resp["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings, key=lambda e: e["index"])

        # Return just the embeddings
        return cast(Embeddings, [result["embedding"]["Float"] for result in sorted_embeddings])
