import logging
from typing import Any

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class CohereEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, api_key: str, model_name: str = "large"):
        try:
            import cohere
        except ImportError:
            raise ValueError(
                "The cohere python package is not installed. Please install it with `pip install cohere`"
            )

        self._client = cohere.Client(api_key)
        self._model_name = model_name

    @staticmethod
    def _convert_v5_embeddings(embeddings_response: Any) -> Embeddings:
        try:
            # works with v5 - does't work with typed returned e.g. int/uint etc.
            from cohere.types.embed_response import (  # noqa: F401
                EmbedResponse_EmbeddingsFloats,
            )

            return embeddings_response.embeddings
        except ImportError:
            # work with v4
            return [embeddings for embeddings in embeddings_response]

    def __call__(self, input: Documents) -> Embeddings:
        # Call Cohere Embedding API for each document.
        embeddings_response = self._client.embed(
            texts=input, model=self._model_name, input_type="search_document"
        )
        return self._convert_v5_embeddings(embeddings_response)
