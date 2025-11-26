from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any, Union, Optional
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.embedding_functions.utils import _get_shared_system_client
from enum import Enum


class ChromaCloudQwenEmbeddingModel(Enum):
    QWEN3_EMBEDDING_0p6B = "Qwen/Qwen3-Embedding-0.6B"


class ChromaCloudQwenEmbeddingTarget(Enum):
    DOCUMENTS = "documents"
    QUERY = "query"


ChromaCloudQwenEmbeddingInstructions = Dict[
    str, Dict[ChromaCloudQwenEmbeddingTarget, str]
]

CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS: ChromaCloudQwenEmbeddingInstructions = {
    "nl_to_code": {
        ChromaCloudQwenEmbeddingTarget.DOCUMENTS: "",
        # Taken from https://github.com/QwenLM/Qwen3-Embedding/blob/main/evaluation/task_prompts.json
        ChromaCloudQwenEmbeddingTarget.QUERY: "Given a question about coding, retrieval code or passage that can solve user's question",
    }
}


class ChromaCloudQwenEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        model: ChromaCloudQwenEmbeddingModel,
        task: Optional[str],
        instructions: ChromaCloudQwenEmbeddingInstructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS,
        api_key_env_var: str = "CHROMA_API_KEY",
    ):
        """
        Initialize the ChromaCloudQwenEmbeddingFunction.

        Args:
            model (ChromaCloudQwenEmbeddingModel): The specific Qwen model to use for embeddings.
            task (str, optional): The task for which embeddings are being generated. If None or empty,
                empty instructions will be used for both documents and queries.
            instructions (ChromaCloudQwenEmbeddingInstructions, optional): A dictionary containing
                custom instructions to use for the specified Qwen model. Defaults to CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS.
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
        # First, try to get API key from environment variable
        self.api_key = os.getenv(api_key_env_var)
        # If not found in env var, try to get it from existing client instances
        if not self.api_key:
            SharedSystemClient = _get_shared_system_client()
            self.api_key = SharedSystemClient.get_chroma_cloud_api_key_from_clients()
        # Raise error if still no API key found
        if not self.api_key:
            raise ValueError(
                f"API key not found in environment variable {api_key_env_var} "
                f"or in any existing client instances"
            )

        self.model = model
        self.task = task
        self.instructions = instructions

        self._api_url = "https://embed.trychroma.com"
        self._session = httpx.Client()
        self._session.headers.update(
            {
                "x-chroma-token": self.api_key,
                "x-chroma-embedding-model": self.model.value,
            }
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

        instruction = ""
        if self.task and self.task in self.instructions:
            instruction = self.instructions[self.task][
                ChromaCloudQwenEmbeddingTarget.DOCUMENTS
            ]

        payload: Dict[str, Union[str, Documents]] = {
            "instructions": instruction,
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

        instruction = ""
        if self.task and self.task in self.instructions:
            instruction = self.instructions[self.task][
                ChromaCloudQwenEmbeddingTarget.QUERY
            ]

        payload: Dict[str, Union[str, Documents]] = {
            "instructions": instruction,
            "texts": input,
        }

        response = self._session.post(self._api_url, json=payload, timeout=60).json()

        return self._parse_response(response)

    @staticmethod
    def name() -> str:
        return "chroma-cloud-qwen"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model = config.get("model")
        task = config.get("task")
        instructions = config.get("instructions")
        api_key_env_var = config.get("api_key_env_var")

        if model is None or task is None:
            assert False, "Config is missing a required field"

        # Deserialize instructions dict from string keys to enum keys
        deserialized_instructions = CHROMA_CLOUD_QWEN_DEFAULT_INSTRUCTIONS
        if instructions is not None:
            deserialized_instructions = {}
            for task_key, targets in instructions.items():
                deserialized_instructions[task_key] = {}
                for target_key, instruction in targets.items():
                    # Convert string key to enum
                    target_enum = ChromaCloudQwenEmbeddingTarget(target_key)
                    deserialized_instructions[task_key][target_enum] = instruction
                    deserialized_instructions[task_key][target_enum] = instruction

        return ChromaCloudQwenEmbeddingFunction(
            model=ChromaCloudQwenEmbeddingModel(model),
            task=task,
            instructions=deserialized_instructions,
            api_key_env_var=api_key_env_var or "CHROMA_API_KEY",
        )

    def get_config(self) -> Dict[str, Any]:
        # Serialize instructions dict with enum keys to string keys for JSON compatibility
        serialized_instructions = {
            task: {target.value: instruction for target, instruction in targets.items()}
            for task, targets in self.instructions.items()
        }
        return {
            "api_key_env_var": self.api_key_env_var,
            "model": self.model.value,
            "task": self.task,
            "instructions": serialized_instructions,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model" in new_config:
            raise ValueError(
                "The model cannot be changed after the embedding function has been initialized."
            )
        elif "task" in new_config:
            raise ValueError(
                "The task cannot be changed after the embedding function has been initialized."
            )
        elif "instructions" in new_config:
            raise ValueError(
                "The instructions cannot be changed after the embedding function has been initialized."
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
        validate_config_schema(config, "chroma-cloud-qwen")
