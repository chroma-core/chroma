from chromadb.api.types import (
    Embeddings,
    Documents,
    EmbeddingFunction,
    Space,
)
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, TypedDict, Optional
import os
import numpy as np


class NomicQueryConfig(TypedDict):
    task_type: str


class NomicEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the Nomic API.
    """

    def __init__(
        self,
        model: str,
        task_type: str,
        query_config: Optional[NomicQueryConfig],
        api_key_env_var: str = "NOMIC_API_KEY",
    ):
        """
        Initialize the NomicEmbeddingFunction.

        Args:
            model (str): The name of the model to use for text embeddings.
            task_type (str): The type of task to embed with. See reference https://docs.nomic.ai/platform/embeddings-and-retrieval/text-embedding#embedding-task-types
            query_config (Optional[NomicQueryConfig]): The configuration for setting task type for queries
            api_key_env_var (str): The environment variable name for the Nomic API key. Defaults to "NOMIC_API_KEY".

            Supported task types: search_document, search_query, classification, clustering
        """
        try:
            from nomic import embed
        except ImportError:
            raise ValueError(
                "The nomic python package is not installed. Please install it with `pip install nomic`"
            )

        self.model = model
        self.task_type = task_type
        self.api_key_env_var = api_key_env_var
        self.api_key = os.getenv(api_key_env_var)
        self.query_config = query_config
        if not self.api_key:
            raise ValueError(f"The {api_key_env_var} environment variable is not set.")
        self.embed = embed

    def __call__(self, input: Documents) -> Embeddings:
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Nomic only supports text documents, not images")
        output = self.embed.text(
            model=self.model,
            texts=input,
            task_type=self.task_type,
        )
        return [np.array(data.embedding) for data in output.data]

    def embed_query(self, input: Documents) -> Embeddings:
        if not all(isinstance(item, str) for item in input):
            raise ValueError("Nomic only supports text queries, not images")

        task_type = (
            self.query_config.get("task_type") if self.query_config else self.task_type
        )
        output = self.embed.text(
            model=self.model,
            texts=input,
            task_type=task_type,
        )
        return [np.array(data.embedding) for data in output.data]

    @staticmethod
    def name() -> str:
        return "nomic"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model = config.get("model")
        api_key_env_var = config.get("api_key_env_var")
        task_type = config.get("task_type")
        query_config = config.get("query_config")
        if model is None or api_key_env_var is None or task_type is None:
            assert False, "This code should not be reached"  # this is for type checking
        return NomicEmbeddingFunction(
            model=model,
            api_key_env_var=api_key_env_var,
            task_type=task_type,
            query_config=query_config,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model": self.model,
            "api_key_env_var": self.api_key_env_var,
            "task_type": self.task_type,
            "query_config": self.query_config,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model" in new_config:
            raise ValueError(
                "The model cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate
        """
        validate_config_schema(config, "nomic")
