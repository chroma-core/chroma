from chromadb.embedding_functions.embedding_function import EmbeddingFunction, Space
from chromadb.api.types import Embeddings, Documents
from typing import List, Dict, Any
import os
import numpy as np


class VoyageAIEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the VoyageAI API.
    """

    def __init__(
        self,
        api_key_env_var: str = "VOYAGE_API_KEY",
        model_name: str = "voyage-large-2",
    ):
        """
        Initialize the VoyageAIEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the VoyageAI API.
                Defaults to "VOYAGE_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "voyage-large-2".
        """
        try:
            import voyageai
        except ImportError:
            raise ValueError(
                "The voyageai python package is not installed. Please install it with `pip install voyageai`"
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.model_name = model_name

        self._client = voyageai.Client(api_key=self.api_key)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        embeddings = self._client.embed(texts=input, model=self.model_name)

        # Convert to numpy arrays
        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    def name(self) -> str:
        return "voyageai"

    def default_space(self) -> Space:
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var", "VOYAGE_API_KEY")
        model_name = config.get("model_name", "voyage-large-2")

        return VoyageAIEmbeddingFunction(
            api_key_env_var=api_key_env_var, model_name=model_name
        )

    def get_config(self) -> Dict[str, Any]:
        return {"api_key_env_var": self.api_key_env_var, "model_name": self.model_name}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )

    def validate_config(self, config: Dict[str, Any]) -> None:
        # TODO: Validate with JSON schema
        pass
