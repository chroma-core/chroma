from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Optional
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema


class CohereEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "large",
        api_key_env_var: str = "CHROMA_COHERE_API_KEY",
    ):
        try:
            import cohere
        except ImportError:
            raise ValueError(
                "The cohere python package is not installed. Please install it with `pip install cohere`"
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.model_name = model_name

        self.client = cohere.Client(self.api_key)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Cohere only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Cohere only supports text documents, not images")

        return [
            np.array(embeddings, dtype=np.float32)
            for embeddings in self.client.embed(
                texts=[str(item) for item in input],
                model=self.model_name,
                input_type="search_document",
            ).embeddings
        ]

    @staticmethod
    def name() -> str:
        return "cohere"

    def default_space(self) -> Space:
        if self.model_name == "embed-multilingual-v2.0":
            return "ip"
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        if self.model_name == "embed-english-v3.0":
            return ["cosine", "l2", "ip"]
        elif self.model_name == "embed-english-light-v3.0":
            return ["cosine", "ip", "l2"]
        elif self.model_name == "embed-english-v2.0":
            return ["cosine"]
        elif self.model_name == "embed-english-light-v2.0":
            return ["cosine"]
        elif self.model_name == "embed-multilingual-v3.0":
            return ["cosine", "l2", "ip"]
        elif self.model_name == "embed-multilingual-light-v3.0":
            return ["cosine", "l2", "ip"]
        elif self.model_name == "embed-multilingual-v2.0":
            return ["ip"]
        else:
            return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return CohereEmbeddingFunction(
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

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "cohere")
