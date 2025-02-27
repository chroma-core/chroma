from chromadb.utils.embedding_functions.embedding_function import (
    EmbeddingFunction,
    Space,
)
from chromadb.utils.embedding_functions.schemas import validate_config
from chromadb.api.types import Embeddings, Documents
from typing import List, Dict, Any
import numpy as np


class Text2VecEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the Text2Vec model.
    """

    def __init__(self, model_name: str = "shibing624/text2vec-base-chinese"):
        """
        Initialize the Text2VecEmbeddingFunction.

        Args:
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "shibing624/text2vec-base-chinese".
        """
        try:
            from text2vec import SentenceModel
        except ImportError:
            raise ValueError(
                "The text2vec python package is not installed. Please install it with `pip install text2vec`"
            )

        self.model_name = model_name
        self._model = SentenceModel(model_name_or_path=model_name)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Text2Vec only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Text2Vec only supports text documents, not images")

        embeddings = self._model.encode(list(input), convert_to_numpy=True)

        # Convert to numpy arrays
        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    @staticmethod
    def name() -> str:
        return "text2vec"

    def default_space(self) -> Space:
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")

        if model_name is None:
            assert False, "This code should not be reached"

        return Text2VecEmbeddingFunction(model_name=model_name)

    def get_config(self) -> Dict[str, Any]:
        return {"model_name": self.model_name}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config(config, "text2vec")
