import os
from enum import Enum
from typing import Optional, cast

from chromadb.api.types import (
    Documents,
    EmbeddingFunction,
    Embeddings,
)


class VoyageAIEmbeddingFunction(EmbeddingFunction[Documents]):
    """Embedding function for Voyageai.com. API docs - https://docs.voyageai.com/reference/embeddings-api"""

    class InputType(str, Enum):
        DOCUMENT = "document"
        QUERY = "query"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "voyage-2",
        max_batch_size: int = 128,
        truncation: Optional[bool] = True,
        input_type: Optional[InputType] = None,
    ):
        """
        Initialize the VoyageAIEmbeddingFunction.
        Args:
        api_key (str): Your API key for the HuggingFace API.
        model_name (str, optional): The name of the model to use for text embeddings. Defaults to "voyage-01".
        batch_size (int, optional): The number of documents to send at a time. Defaults to 128 (The max supported 7th Apr 2024). see voyageai.VOYAGE_EMBED_BATCH_SIZE for actual max.
        truncation (bool, optional): Whether to truncate the input (`True`) or raise an error if the input is too long (`False`). Defaults to `False`.
        input_type (str, optional): The type of input text. Can be `None`, `query`, `document`. Defaults to `None`.
        """

        if not api_key and "VOYAGE_API_KEY" not in os.environ:
            raise ValueError("Please provide a VoyageAI API key.")

        try:
            import voyageai

            if max_batch_size > voyageai.VOYAGE_EMBED_BATCH_SIZE:
                raise ValueError(
                    f"The maximum batch size supported is {voyageai.VOYAGE_EMBED_BATCH_SIZE}."
                )
            self._batch_size = max_batch_size
            self._model = model_name
            self._truncation = truncation
            self._client = voyageai.Client(api_key=api_key)
            self._input_type = input_type
        except ImportError:
            raise ValueError(
                "The VoyageAI python package is not installed. Please install it with `pip install voyageai`"
            )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.
        Args:
        input (Documents): A list of texts to get embeddings for.
        Returns:
        Embeddings: The embeddings for the texts.
        Example:
        >>> voyage_ef = VoyageAIEmbeddingFunction(api_key="your_api_key")
        >>> input = ["Hello, world!", "How are you?"]
        >>> embeddings = voyage_ef(input)
        """
        if len(input) > self._batch_size:
            raise ValueError(f"The maximum batch size supported is {self._batch_size}.")
        results = self._client.embed(
            texts=input,
            model=self._model,
            truncation=self._truncation,
            input_type=self._input_type,
        )
        return cast(Embeddings, results.embeddings)
