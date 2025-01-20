import logging
from typing import Union, cast, Optional
from urllib.parse import urlparse

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)

DEFAULT_MODEL_NAME = "chroma/all-minilm-l6-v2-f32"


class OllamaEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the Ollama Embedding API (https://github.com/ollama/ollama/blob/main/docs/api.md#generate-embeddings).
    """

    def __init__(
        self,
        url: Optional[str] = "http://localhost:11434",
        model_name: Optional[str] = DEFAULT_MODEL_NAME,
        *,
        timeout: Optional[int] = 60,
    ) -> None:
        """
        Initialize the Ollama Embedding Function.

        Args:
            url (str): The Base URL of the Ollama Server (default: "http://localhost:11434").
            model_name (str): The name of the model to use for text embeddings. E.g. "nomic-embed-text" (see defaults to "chroma/all-minilm-l6-v2-f32", for available models see https://ollama.com/library).
        """

        try:
            from ollama import Client
        except ImportError:
            raise ValueError(
                "The ollama python package is not installed. Please install it with `pip install ollama`"
            )
        # adding this for backwards compatibility with the old version of the EF
        self._base_url = url
        if self._base_url.endswith("/api/embeddings"):
            parsed_url = urlparse(url)
            self._base_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        self._client = Client(host=self._base_url, timeout=timeout)
        self._model_name = model_name or DEFAULT_MODEL_NAME

    def __call__(self, input: Union[Documents, str]) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> ollama_ef = OllamaEmbeddingFunction()
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = ollama_ef(texts)
        """
        # Call Ollama client
        response = self._client.embed(model=self._model_name, input=input)
        return cast(Embeddings, response["embeddings"])
