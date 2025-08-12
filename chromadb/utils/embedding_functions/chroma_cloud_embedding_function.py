from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Optional
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import warnings
from enum import Enum

class ChromaCloudEmbeddingModel(Enum):
    BGE_M3 = "baai/bge-m3"

class ChromaCloudEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        model: str,
        tenant_uuid: str,
        api_key: Optional[str] = None,
        api_key_env_var: str = "CHROMA_API_KEY",
        timeout_seconds: int = 120,
    ):
        """
        Initialize the ChromaCloudEmbeddingFunction.

        Args:
            model: (ChromaCloudEmbeddingModel) the
            api_key (str, optional): The API key for the Chroma API. If not provided,
                it will be read from the environment variable specified by api_key_env_var.
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "CHROMA_API_KEY".
            timeout_seconds (int, optional): The time in seconds to wait for a response from the Chroma Cloud EF.
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`."
            )
        
        try:
            validated_model = ChromaCloudEmbeddingModel(model)
        except:
            raise ValueError("The valid ")

        if api_key is not None:
            warnings.warn(
                "Direct api_key configuration will not be persisted. "
                "Please use environment variables via api_key_env_var for persistent storage.",
                DeprecationWarning,
            )

        self.model = validated_model
        self.tenant_uuid = tenant_uuid
        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self._api_url = "https://chroma-core-staging--chroma-trieve-ingest-immediateembed-10d1fc.modal.run"
        self._session = httpx.Client(timeout=timeout_seconds)
        self._session.headers.update(
            {"x-chroma-token": self.api_key}
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

        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`."
            )
        
        try:
            response = self._session.post(
                self._api_url,
                json={"model": str(self.model.value), "texts": input, "tenant_uuid": self.tenant_uuid},
            )
            response.raise_for_status()
            response_data = response.json()
        except httpx.RequestError as e:
            raise ValueError(f"Failed to connect to Chroma Cloud API: {e}")
        except httpx.HTTPStatusError as e:
            raise ValueError(f"Chroma Cloud API returned error {e.response.status_code}: {e.response.text}")
        except Exception as e:
            raise ValueError(f"Failed to parse API response: {e}")
        
        # Extract embeddings from response
        if not hasattr(response_data, 'data') or not response_data.data:
            raise ValueError("Invalid response format from Chroma Cloud API")

        return [np.array(data.embedding, dtype=np.float32) for data in response_data.data]

    @staticmethod
    def name() -> str:
        return "chroma_cloud"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        # Extract parameters from config
        model = config.get("model")
        tenant_uuid = config.get("tenant_uuid")
        api_key_env_var = config.get("api_key_env_var")

        if model is None or tenant_uuid is None or api_key_env_var is None:
            assert False, "This code should not be reached"

        # Create and return the embedding function
        return ChromaCloudEmbeddingFunction(
            model=model,
            tenant_uuid=tenant_uuid,
            api_key_env_var=api_key_env_var,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model" in new_config:
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
        validate_config_schema(config, "chroma_cloud")