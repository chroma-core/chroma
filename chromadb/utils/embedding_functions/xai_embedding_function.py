import asyncio
import logging
from typing import Optional

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)

class XAIEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, model_name: str, api_key: Optional[str] = None, host: str = "api.x.ai"):
        """
        Initialize the XAIEmbeddingFunction.
        Args:
            model_name (str): The name of the model to use for embedding.
            api_key (str, optional): Your API key for the xai-sdk. If not
                provided, it will raise an error to provide an xAI API key.
            host (str, optional): Hostname of the xAI API server.
        """
        try:
            import xai_sdk
        except ImportError:
            raise ValueError(
                "The xai-sdk python package is not installed. Please install it with `pip install xai-sdk`"
            )

        if api_key is None:
            raise ValueError("Please provide an OpenAI API key. You can get one at https://developers.x.ai/api/api-key/")

        self._api_key = api_key
        self._host = host
        self._model_name = model_name
        self._client = xai_sdk.Client(api_key=self._api_key, api_host=self._host)

    def __call__(self, input: Documents) -> Embeddings:
        # embed() returns a list of tuples, where each contains the embedding and its shape
        embeddings = asyncio.run(self._client.embedder.embed(texts=input, model_name=self._model_name))
        return [embedding for embedding, _ in embeddings]
