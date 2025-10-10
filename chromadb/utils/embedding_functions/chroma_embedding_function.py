from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Optional, Union
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import warnings


class ChromaEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_id: str = "Qwen/Qwen3-Embedding-0.6B",
        task: str = "code",
        api_key_env_var: str = "CHROMA_API_KEY",
    ):
        """
        Initialize the ChromaEmbeddingFunction.

        Args:
            api_key (str, optional): The API key for Chroma. If not provided,
                it will be read from the environment variable specified by api_key_env_var.
            model_id (str, optional): The ID of the model to use for embeddings.
                Defaults to "Qwen/Qwen3-Embedding-0.6B". Supported values are:
                - Qwen/Qwen3-Embedding-0.6B
                - BAAI/bge-m3
            task (str, optional): The task for which embeddings are being generated.
                Defaults to "code". Supported values are:
                - code
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "CHROMA_API_KEY".
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
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

        self.model_id = model_id
        self.task = task

        self._api_url = "https://embed.trychroma.com"
        self._session = httpx.Client()
        self._session.headers.update(
            {"x-chroma-token": self.api_key, "x-chroma-embedding-model": self.model_id}
        )

    def _parse_response(self, response: Any) -> Embeddings:
        """
        Convert the response from the Chroma Embedding API to a list of numpy arrays.

        Args:
            response (Any): The response from the Chroma Embedding API.

        Returns:
            Embeddings: A list of numpy arrays representing the embeddings.
        """
        if "embeddings" not in response:
            raise RuntimeError(response.get("error", "Unknown error"))

        embeddings: List[List[float]] = response["embeddings"]

        return np.array(embeddings, dtype=np.float32)


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

        payload: Dict[str, Union[str, Documents]] = {
            "target": "documents",
            "task": self.task,
            "texts": input,
        }

        response = self._session.post(self._api_url, json=payload, timeout=60).json()

        return self._parse_response(response)

    def embed_query(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a query input.
        """
        if not input:
            return []

        payload: Dict[str, Union[str, Documents]] = {
            "target": "query",
            "task": self.task,
            "texts": input,
        }

        response = self._session.post(self._api_url, json=payload, timeout=60).json()

        return self._parse_response(response)

    @staticmethod
    def name() -> str:
        return "chroma-embed"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_id = config.get("model_id")
        task = config.get("task")

        if api_key_env_var is None or model_id is None:
            assert False, "This code should not be reached"

        return ChromaEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_id=model_id,
            task=task if task is not None else "code",
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_id": self.model_id,
            "task": self.task,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_id" in new_config:
            raise ValueError(
                "The model cannot be changed after the embedding function has been initialized."
            )
        elif "task" in new_config:
            raise ValueError(
                "The task cannot be changed after the embedding function has been initialized."
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
        validate_config_schema(config, "chroma-embed")