from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Optional
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import warnings


class MorphEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "morph-embedding-v2",
        api_base: str = "https://api.morphllm.com/v1",
        encoding_format: str = "float",
        api_key_env_var: str = "MORPH_API_KEY",
    ):
        """
        Initialize the MorphEmbeddingFunction.

        Args:
            api_key (str, optional): The API key for the Morph API. If not provided,
                it will be read from the environment variable specified by api_key_env_var.
            model_name (str, optional): The name of the model to use for embeddings.
                Defaults to "morph-embedding-v2".
            api_base (str, optional): The base URL for the Morph API.
                Defaults to "https://api.morphllm.com/v1".
            encoding_format (str, optional): The format for embeddings (float or base64).
                Defaults to "float".
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "MORPH_API_KEY".
        """
        try:
            import openai
        except ImportError:
            raise ValueError(
                "The openai python package is not installed. Please install it with `pip install openai`. "
                "Note: Morph uses the OpenAI client library for API communication."
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.model_name = model_name
        self.api_base = api_base
        self.encoding_format = encoding_format

        # Initialize the OpenAI client with Morph's base URL
        self.client = openai.OpenAI(
            api_key=self.api_key,
            base_url=self.api_base,
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        # Handle empty input
        if not input:
            return []

        # Prepare embedding parameters
        embedding_params: Dict[str, Any] = {
            "model": self.model_name,
            "input": input,
            "encoding_format": self.encoding_format,
        }

        # Get embeddings from Morph API
        response = self.client.embeddings.create(**embedding_params)

        # Extract embeddings from response
        return [np.array(data.embedding, dtype=np.float32) for data in response.data]

    @staticmethod
    def name() -> str:
        return "morph"

    def default_space(self) -> Space:
        # Morph embeddings work best with cosine similarity
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        # Extract parameters from config
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        api_base = config.get("api_base")
        encoding_format = config.get("encoding_format")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        # Create and return the embedding function
        return MorphEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            api_base=api_base if api_base is not None else "https://api.morphllm.com/v1",
            encoding_format=encoding_format if encoding_format is not None else "float",
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "api_base": self.api_base,
            "encoding_format": self.encoding_format,
        }

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
        validate_config_schema(config, "morph")