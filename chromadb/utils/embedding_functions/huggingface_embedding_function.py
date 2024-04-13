import logging

from chromadb.api.types import (
    Documents,
    EmbeddingFunction,
    Embeddings,
)

from typing import cast
import requests

logger = logging.getLogger(__name__)



class HuggingFaceEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the HuggingFace API.
    It requires an API key and a model name. The default model name is "sentence-transformers/all-MiniLM-L6-v2".
    """

    def __init__(
        self, api_key: str, model_name: str = "sentence-transformers/all-MiniLM-L6-v2"
    ):
        """
        Initialize the HuggingFaceEmbeddingFunction.

        Args:
            api_key (str): Your API key for the HuggingFace API.
            model_name (str, optional): The name of the model to use for text embeddings. Defaults to "sentence-transformers/all-MiniLM-L6-v2".
        """
        self._api_url = f"https://api-inference.huggingface.co/pipeline/feature-extraction/{model_name}"
        self._session = requests.Session()
        self._session.headers.update({"Authorization": f"Bearer {api_key}"})

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            texts (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> hugging_face = HuggingFaceEmbeddingFunction(api_key="your_api_key")
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = hugging_face(texts)
        """
        # Call HuggingFace Embedding API for each document
        return cast(
            Embeddings,
            self._session.post(
                self._api_url,
                json={"inputs": input, "options": {"wait_for_model": True}},
            ).json(),
        )



class HuggingFaceEmbeddingServer(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the HuggingFace Embedding server (https://github.com/huggingface/text-embeddings-inference).
    The embedding model is configured in the server.
    """

    def __init__(self, url: str):
        """
        Initialize the HuggingFaceEmbeddingServer.

        Args:
            url (str): The URL of the HuggingFace Embedding Server.
        """
        try:
            import requests
        except ImportError:
            raise ValueError(
                "The requests python package is not installed. Please install it with `pip install requests`"
            )
        self._api_url = f"{url}"
        self._session = requests.Session()

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            texts (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> hugging_face = HuggingFaceEmbeddingServer(url="http://localhost:8080/embed")
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = hugging_face(texts)
        """
        # Call HuggingFace Embedding Server API for each document
        return cast(
            Embeddings, self._session.post(self._api_url, json={"inputs": input}).json()
        )