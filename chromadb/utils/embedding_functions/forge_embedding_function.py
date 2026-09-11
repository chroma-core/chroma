from chromadb.api.types import EmbeddingFunction, Space, Embeddings, Documents
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Optional
import os
import numpy as np
import warnings


class ForgeEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the
    Forge API (https://voxell.ai).

    Forge exposes an OpenAI-compatible embeddings endpoint, so this function
    uses the ``openai`` client configured with the Forge base URL.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "forge-pro",
        api_key_env_var: str = "FORGE_API_KEY",
        api_base: str = "https://api.voxell.ai/v1",
        dimensions: Optional[int] = None,
    ):
        """
        Initialize the ForgeEmbeddingFunction.

        Args:
            api_key (str, optional): API key for the Forge API. If not provided,
                will look for it in the environment variable.
            model_name (str, optional): The name of the model to use for text
                embeddings. Defaults to "forge-pro". Available models:
                "forge-turbo" (1024d), "forge-pro" (2560d), "forge-ultra-4k" (4096d).
            api_key_env_var (str, optional): Environment variable name that contains
                your API key for the Forge API. Defaults to "FORGE_API_KEY".
            api_base (str, optional): The base URL for the Forge API.
                Defaults to "https://api.voxell.ai/v1".
            dimensions (int, optional): Forge embeddings support Matryoshka
                representation learning, allowing you to reduce embedding dimensions
                while maintaining quality.
        """
        try:
            import openai
        except ImportError:
            raise ValueError(
                "The openai python package is not installed. Please install it with `pip install openai`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.model_name = model_name
        self.api_base = api_base
        self.dimensions = dimensions
        self.client = openai.OpenAI(api_key=self.api_key, base_url=self.api_base)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        if not input:
            return []

        embedding_params: Dict[str, Any] = {
            "model": self.model_name,
            "input": input,
        }

        if self.dimensions is not None:
            embedding_params["dimensions"] = self.dimensions

        response = self.client.embeddings.create(**embedding_params)

        return [np.array(data.embedding, dtype=np.float32) for data in response.data]

    @staticmethod
    def name() -> str:
        return "forge"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        api_base = config.get("api_base") or "https://api.voxell.ai/v1"
        dimensions = config.get("dimensions")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return ForgeEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            api_base=api_base,
            dimensions=dimensions,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "api_base": self.api_base,
            "dimensions": self.dimensions,
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
        validate_config_schema(config, "forge")
