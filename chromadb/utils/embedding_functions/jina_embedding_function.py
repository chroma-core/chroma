from chromadb.api.types import (
    Embeddings,
    EmbeddingFunction,
    Space,
    Embeddable,
    is_image,
    is_document,
)
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Union, Optional, TypedDict
import os
import numpy as np
import warnings
import importlib
import base64
import io


class JinaQueryConfig(TypedDict):
    task: str


class JinaEmbeddingFunction(EmbeddingFunction[Embeddable]):
    """
    This class is used to get embeddings for a list of texts using the Jina AI API.
    It requires an API key and a model name. The default model name is "jina-embeddings-v2-base-en".
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "jina-embeddings-v2-base-en",
        api_key_env_var: str = "CHROMA_JINA_API_KEY",
        task: Optional[str] = None,
        late_chunking: Optional[bool] = None,
        truncate: Optional[bool] = None,
        dimensions: Optional[int] = None,
        embedding_type: Optional[str] = None,
        normalized: Optional[bool] = None,
        query_config: Optional[JinaQueryConfig] = None,
    ):
        """
        Initialize the JinaEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the Jina AI API.
                Defaults to "CHROMA_JINA_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "jina-embeddings-v2-base-en".
            task (str, optional): The task to use for the Jina AI API.
                Defaults to None.
            late_chunking (bool, optional): Whether to use late chunking for the Jina AI API.
                Defaults to None.
            truncate (bool, optional): Whether to truncate the Jina AI API.
                Defaults to None.
            dimensions (int, optional): The number of dimensions to use for the Jina AI API.
                Defaults to None.
            embedding_type (str, optional): The type of embedding to use for the Jina AI API.
                Defaults to None.
            normalized (bool, optional): Whether to normalize the Jina AI API.
                Defaults to None.

        """
        try:
            import httpx
        except ImportError:
            raise ValueError(
                "The httpx python package is not installed. Please install it with `pip install httpx`"
            )
        try:
            self._PILImage = importlib.import_module("PIL.Image")
        except ImportError:
            raise ValueError(
                "The PIL python package is not installed. Please install it with `pip install pillow`"
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

        # Initialize optional attributes to None
        self.task = task
        self.late_chunking = late_chunking
        self.truncate = truncate
        self.dimensions = dimensions
        self.embedding_type = embedding_type
        self.normalized = normalized
        self.query_config = query_config

        self._api_url = "https://api.jina.ai/v1/embeddings"
        self._session = httpx.Client()
        self._session.headers.update(
            {"Authorization": f"Bearer {self.api_key}", "Accept-Encoding": "identity"}
        )

    def _build_payload(self, input: Embeddable, is_query: bool) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "input": [],
            "model": self.model_name,
        }
        if all(is_document(item) for item in input):
            payload["input"] = input
        else:
            for item in input:
                if is_document(item):
                    payload["input"].append({"text": item})
                elif is_image(item):
                    try:
                        pil_image = self._PILImage.fromarray(item)

                        buffer = io.BytesIO()
                        pil_image.save(buffer, format="PNG")
                        img_bytes = buffer.getvalue()

                        # Encode bytes to base64 string
                        base64_string = base64.b64encode(img_bytes).decode("utf-8")

                    except Exception as e:
                        raise ValueError(
                            f"Failed to convert image numpy array to base64 data URI: {e}"
                        ) from e
                    payload["input"].append({"image": base64_string})

        if self.task is not None:
            payload["task"] = self.task
        if self.late_chunking is not None:
            payload["late_chunking"] = self.late_chunking
        if self.truncate is not None:
            payload["truncate"] = self.truncate
        if self.dimensions is not None:
            payload["dimensions"] = self.dimensions
        if self.embedding_type is not None:
            payload["embedding_type"] = self.embedding_type
        if self.normalized is not None:
            payload["normalized"] = self.normalized

        # overwrite parameteres when query payload is used
        if is_query and self.query_config is not None:
            for key, value in self.query_config.items():
                payload[key] = value

        return payload

    def _convert_resp(self, resp: Any, is_query: bool = False) -> Embeddings:
        """
        Convert the response from the Jina AI API to a list of numpy arrays.

        Args:
            resp (Any): The response from the Jina AI API.

        Returns:
            Embeddings: A list of numpy arrays representing the embeddings.
        """
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

    def __call__(self, input: Embeddable) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Embeddable): A list of texts and/or images to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> jina_ai_fn = JinaEmbeddingFunction(api_key_env_var="CHROMA_JINA_API_KEY")
            >>> input = ["Hello, world!", "How are you?"]
        """

        payload = self._build_payload(input, is_query=False)

        # Call Jina AI Embedding API
        resp = self._session.post(self._api_url, json=payload, timeout=60).json()

        return self._convert_resp(resp)

    def embed_query(self, input: Embeddable) -> Embeddings:
        payload = self._build_payload(input, is_query=True)

        # Call Jina AI Embedding API
        resp = self._session.post(self._api_url, json=payload, timeout=60).json()

        return self._convert_resp(resp, is_query=True)

    @staticmethod
    def name() -> str:
        return "jina"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Embeddable]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        task = config.get("task")
        late_chunking = config.get("late_chunking")
        truncate = config.get("truncate")
        dimensions = config.get("dimensions")
        embedding_type = config.get("embedding_type")
        normalized = config.get("normalized")
        query_config = config.get("query_config")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"  # this is for type checking

        return JinaEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            task=task,
            late_chunking=late_chunking,
            truncate=truncate,
            dimensions=dimensions,
            embedding_type=embedding_type,
            normalized=normalized,
            query_config=query_config,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "task": self.task,
            "late_chunking": self.late_chunking,
            "truncate": self.truncate,
            "dimensions": self.dimensions,
            "embedding_type": self.embedding_type,
            "normalized": self.normalized,
            "query_config": self.query_config,
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
        validate_config_schema(config, "jina")
