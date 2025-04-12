from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Union, Optional
import os
import numpy as np
import httpx


class JinaEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the Jina AI API.
    It requires an API key and a model name. The default model name is "jina-embeddings-v3".
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "jina-embeddings-v3",
        api_key_env_var: str = "CHROMA_JINA_API_KEY",
        task: str = "text-matching",
        late_chunking: bool = False,
        dimensions: int = 1024,
        embedding_type: str = "float",
    ):
        """
        Initialize the JinaEmbeddingFunction.

        Args:
            api_key (str, optional): Your API key for the Jina AI API. Defaults to None (uses env variable).
            model_name (str, optional): The name of the model to use for text embeddings. Defaults to "jina-embeddings-v3".
            api_key_env_var (str, optional): Environment variable containing the API key. Defaults to "CHROMA_JINA_API_KEY".
            task (str, optional): The model will generate optimized embeddings for that task. Defaults to "text-matching".
            late_chunking (bool, optional): The model will generate contextual chunk embeddings. Defaults to False.
            dimensions (int, optional): Number of dimensions. Defaults to 1024.
            embedding_type (str, optional): Type of embedding to be returned. Defaults to "float".
        """
        self.api_key_env_var = api_key_env_var
        self.api_key = api_key or os.getenv(api_key_env_var)
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")

        self.model_name = model_name
        self.task = task
        self.late_chunking = late_chunking
        self.dimensions = dimensions
        self.embedding_type = embedding_type
        self._api_url = "https://api.jina.ai/v1/embeddings"
        self._session = httpx.Client()
        self._session.headers.update(
            {"Authorization": f"Bearer {self.api_key}", "Accept-Encoding": "identity"}
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.
        """
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Jina AI only supports text documents, not images")

        resp = self._session.post(
            self._api_url,
            json={
                "input": input,
                "model": self.model_name,
                "task": self.task,
                "late_chunking": self.late_chunking,
                "dimensions": self.dimensions,
                "embedding_type": self.embedding_type,
            },
        ).json()

        if "data" not in resp:
            raise RuntimeError(resp.get("detail", "Unknown error"))

        embeddings_data: List[Dict[str, Union[int, List[float]]]] = resp["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings_data, key=lambda e: e["index"])

        return [np.array(result["embedding"], dtype=np.float32) for result in sorted_embeddings]

    @staticmethod
    def name() -> str:
        return "jina"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        return JinaEmbeddingFunction(
            api_key_env_var=config.get("api_key_env_var"),
            model_name=config.get("model_name"),
            task=config.get("task", "text-matching"),
            late_chunking=config.get("late_chunking", False),
            dimensions=config.get("dimensions", 1024),
            embedding_type=config.get("embedding_type", "float"),
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "task": self.task,
            "late_chunking": self.late_chunking,
            "dimensions": self.dimensions,
            "embedding_type": self.embedding_type,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError("The model name cannot be changed after initialization.")

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "jina")
