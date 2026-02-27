from chromadb.api.types import EmbeddingFunction, Space, Embeddings, Documents
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.embedding_functions.utils import decode_embedding
from typing import List, Dict, Any, Optional
import os
import numpy as np
import warnings
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import perplexity


class PerplexityEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using the Perplexity API.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "pplx-embed-v1-0.6b",
        api_key_env_var: str = "PERPLEXITY_API_KEY",
        dimensions: Optional[int] = None,
    ):
        """
        Initialize the PerplexityEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Perplexity API.
                Defaults to "Perplexity_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "pplx-embed-v1-0.6b".
            api_key (str, optional): API key for the Perplexity API. If not provided, will look for it in the environment variable.
            dimensions (int, optional): Perplexity embeddings support Matryoshka representation learning, allowing you
                to reduce embedding dimensions while maintaining quality.
        """
        try:
            import perplexity
        except ImportError:
            raise ValueError(
                "The perplexityai python package is not installed. Please install it with `pip install perplexityai`"
            )

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )

        if os.getenv("PERPLEXITY_API_KEY") is not None:
            self.api_key_env_var = "PERPLEXITY_API_KEY"
        else:
            self.api_key_env_var = api_key_env_var

        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        self.model_name = model_name
        self.dimensions = dimensions
        self._client = perplexity.Perplexity(api_key=self.api_key)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        response = self._client.embeddings.create(
            input=input,
            model=self.model_name,
            dimensions=self.dimensions
        )

        return [decode_embedding(emb.embedding) for emb in response.data]

    @staticmethod
    def name() -> str:
        return "perplexity"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        dimensions = config.get("dimensions")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return PerplexityEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            dimensions=dimensions,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "dimensions": self.input_type,
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
        validate_config_schema(config, "perplexity")
