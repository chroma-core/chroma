import logging
from typing import Union

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class MistralEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the Mistral AI Embeddings API (https://docs.mistral.ai/capabilities/embeddings/)
    """

    def __init__(self, api_key: str, model_name: str) -> None:
        """
        Initialize the Mistral Embedding Function.

        Args:
            api_key (str): The API key for Mistral AI.
            model_name (str): The name of the model to use for text embeddings. E.g. "mistral-embed" (see https://docs.mistral.ai/getting-started/models/ for available models).
        """
        if not api_key:
            raise ValueError("Please provide a Mistral API key.")

        if not model_name:
            raise ValueError("Please provide the model name.")

        try:
            from mistralai.client import MistralClient
        except ImportError:
            raise ValueError(
                "The Mistral AI python package is not installed. Please install it with `pip install mistralai`"
            )

        self._client = MistralClient(api_key=api_key)
        self._model_name = model_name

    def __call__(self, input: Union[Documents, str]) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> mistral_ef = MistralEmbeddingFunction(api_key="your_api_key", model_name="mistral-embed")
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = mistral_ef(texts)
        """
        texts = input if isinstance(input, list) else [input]

        return [
            embedding_obj.embedding
            for embedding_obj in self._client.embeddings(
                model=self._model_name, input=texts 
            ).data
        ]
