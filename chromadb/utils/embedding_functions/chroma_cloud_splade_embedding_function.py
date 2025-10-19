from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseVectors,
    Documents,
)
from typing import Dict, Any
from enum import Enum
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector
from chromadb.base_types import SparseVector
import os
from typing import Union


class ChromaCloudSpladeEmbeddingModel(Enum):
    SPLADE_PP_EN_V1 = "prithivida/Splade_PP_en_v1"


class ChromaCloudSpladeEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    def __init__(
        self,
        api_key_env_var: str = "CHROMA_API_KEY",
        model: ChromaCloudSpladeEmbeddingModel = ChromaCloudSpladeEmbeddingModel.SPLADE_PP_EN_V1,
    ):
        """
        Initialize the ChromaCloudSpladeEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key.
                Defaults to "CHROMA_API_KEY".
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
            )
        self.api_key_env_var = api_key_env_var
        self.api_key = os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"API key not found in environment variable {self.api_key_env_var}"
            )
        self.model = model
        self._api_url = "https://embed.trychroma.com/embed_sparse"
        self._session = httpx.Client()
        self._session.headers.update(
            {
                "x-chroma-token": self.api_key,
                "x-chroma-embedding-model": self.model.value,
            }
        )

    def __del__(self) -> None:
        """
        Cleanup the HTTP client session when the object is destroyed.
        """
        if hasattr(self, "_session"):
            self._session.close()

    def close(self) -> None:
        """
        Explicitly close the HTTP client session.
        Call this method when you're done using the embedding function.
        """
        if hasattr(self, "_session"):
            self._session.close()

    def __call__(self, input: Documents) -> SparseVectors:
        """
        Generate embeddings for the given documents.

        Args:
            input (Documents): The documents to generate embeddings for.
        """
        if not input:
            return []

        payload: Dict[str, Union[str, Documents]] = {
            "texts": list(input),
            "task": "",
            "target": "",
        }

        try:
            import httpx

            response = self._session.post(self._api_url, json=payload, timeout=60)
            response.raise_for_status()
            json_response = response.json()
            return self._parse_response(json_response)
        except httpx.HTTPStatusError as e:
            raise RuntimeError(
                f"Failed to get embeddings from Chroma Cloud API: HTTP {e.response.status_code} - {e.response.text}"
            )
        except httpx.TimeoutException:
            raise RuntimeError("Request to Chroma Cloud API timed out after 60 seconds")
        except httpx.HTTPError as e:
            raise RuntimeError(f"Failed to get embeddings from Chroma Cloud API: {e}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error calling Chroma Cloud API: {e}")

    def _parse_response(self, response: Any) -> SparseVectors:
        """
        Parse the response from the Chroma Cloud Sparse Embedding API.
        """
        raw_embeddings = response["embeddings"]

        # Normalize each sparse vector (sort indices and validate)
        normalized_vectors: SparseVectors = []
        for emb in raw_embeddings:
            # Handle both dict format and SparseVector format
            if isinstance(emb, dict):
                indices = emb.get("indices", [])
                values = emb.get("values", [])
            else:
                # Already a SparseVector, extract its data
                indices = emb.indices
                values = emb.values

            normalized_vectors.append(
                normalize_sparse_vector(indices=indices, values=values)
            )

        return normalized_vectors

    @staticmethod
    def name() -> str:
        return "chroma-cloud-splade"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model = config.get("model")
        if model is None:
            raise ValueError("model must be provided in config")
        if not api_key_env_var:
            raise ValueError("api_key_env_var must be provided in config")
        return ChromaCloudSpladeEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model=ChromaCloudSpladeEmbeddingModel(model),
        )

    def get_config(self) -> Dict[str, Any]:
        return {"api_key_env_var": self.api_key_env_var, "model": self.model.value}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model" in new_config:
            raise ValueError(
                "model cannot be changed after the embedding function has been initialized"
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "chroma-cloud-splade")
