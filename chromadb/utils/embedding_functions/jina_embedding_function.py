from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Union, Optional
import os
import numpy as np


class JinaEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the Jina AI API.
    It requires an API key and a model name. The default model name is "jina-embeddings-v2-base-en".
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "jina-embeddings-v2-base-en",
        api_key_env_var: str = "CHROMA_JINA_API_KEY",
    ):
        """
        Initialize the JinaEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Jina AI API.
                Defaults to "CHROMA_JINA_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "jina-embeddings-v2-base-en".
        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
            )

        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.model_name = model_name

        self._api_url = "https://api.jina.ai/v1/embeddings"
        self._session = httpx.Client()
        self._session.headers.update(
            {"Authorization": f"Bearer {self.api_key}", "Accept-Encoding": "identity"}
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts or images to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> jina_ai_fn = JinaEmbeddingFunction(api_key_env_var="CHROMA_JINA_API_KEY")
            >>> input = ["Hello, world!", "How are you?"]
        """
        # Jina AI only works with text documents
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Jina AI only supports text documents, not images")

        # Call Jina AI Embedding API
        resp = self._session.post(
            self._api_url, json={"input": input, "model": self.model_name}
        ).json()

        if "data" not in resp:
            raise RuntimeError(resp.get("detail", "Unknown error"))

        embeddings_data: List[Dict[str, Union[int, List[float]]]] = resp["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings_data, key=lambda e: e["index"])

        # Return embeddings as numpy arrays
        return [
            np.array(result["embedding"], dtype=np.float32)
            for result in sorted_embeddings
        ]

    @staticmethod
    def name() -> str:
        return "jina"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return JinaEmbeddingFunction(
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
        validate_config_schema(config, "jina")
