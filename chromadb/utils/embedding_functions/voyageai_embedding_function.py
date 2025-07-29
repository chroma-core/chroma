from chromadb.api.types import (
    EmbeddingFunction,
    Space,
    Embeddings,
    Embeddable,
    Image,
    Document,
    is_image,
    is_document,
)

from chromadb.utils.embedding_functions.schemas import validate_config_schema
from typing import List, Dict, Any, Optional, Union
import os
import numpy as np
import warnings
import importlib


class VoyageAIEmbeddingFunction(EmbeddingFunction[Embeddable]):
    """
    This class is used to generate embeddings for a list of texts using the VoyageAI API.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "voyage-large-2",
        api_key_env_var: str = "CHROMA_VOYAGE_API_KEY",
        input_type: Optional[str] = None,
        truncation: bool = True,
        dimensions: Optional[int] = None,
        embedding_type: Optional[str] = None,
    ):
        """
        Initialize the VoyageAIEmbeddingFunction.

        Args:
            api_key_env_var (str, optional): Environment variable name that contains your API key for the VoyageAI API.
                Defaults to "CHROMA_VOYAGE_API_KEY".
            model_name (str, optional): The name of the model to use for text embeddings.
                Defaults to "voyage-large-2".
            api_key (str, optional): API key for the VoyageAI API. If not provided, will look for it in the environment variable.
            input_type (str, optional): The type of input to use for the VoyageAI API.
                Defaults to None.
            truncation (bool): Whether to truncate the input text.
                Defaults to True.
        """
        try:
            import voyageai
        except ImportError:
            raise ValueError(
                "The voyageai python package is not installed. Please install it with `pip install voyageai`"
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
        self.input_type = input_type
        self.truncation = truncation
        self.dimensions = dimensions
        self.embedding_type = embedding_type
        self._client = voyageai.Client(api_key=self.api_key)

    def __call__(self, input: Embeddable) -> Embeddings:
        """
        Generate embeddings for the given documents or images.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents or images.
        """
        if self._is_context_model():
            embeddings = (
                self._client.contextualized_embed(
                    inputs=[input],
                    model=self.model_name,
                    input_type=self.input_type,
                    output_dimension=self.dimensions,
                )
                .results[0]
                .embeddings
            )
        elif self._is_multimodal_model():
            embeddings = self._client.multimodal_embed(
                inputs=[[self.convert(i)] for i in input],
                model=self.model_name,
                input_type=self.input_type,
                truncation=self.truncation,
            ).embeddings
        else:
            embeddings = self._client.embed(
                texts=input,
                model=self.model_name,
                input_type=self.input_type,
                truncation=self.truncation,
                output_dimension=self.dimensions,
            ).embeddings

        # Convert to numpy arrays
        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    def convert(self, embeddable: Union[Image, Document]) -> Any:
        if is_document(embeddable):
            return embeddable
        elif is_image(embeddable):
            # Convert to numpy array and ensure proper dtype for PIL
            image_array = np.array(embeddable)

            # Convert to uint8 if not already, clipping values to valid range
            if image_array.dtype != np.uint8:
                # Normalize to 0-255 range if values are outside uint8 range
                if image_array.max() > 255 or image_array.min() < 0:
                    image_array = np.clip(image_array, 0, 255)
                image_array = image_array.astype(np.uint8)

            return self._PILImage.fromarray(image_array)
        else:
            return None

    def _is_context_model(self) -> bool:
        """Check if the model is a contextualized embedding model."""
        return "context" in self.model_name

    def _is_multimodal_model(self) -> bool:
        """Check if the model is a contextualized embedding model."""
        return "multimodal" in self.model_name

    @staticmethod
    def name() -> str:
        return "voyageai"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Embeddable]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        input_type = config.get("input_type")
        truncation = config.get("truncation")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return VoyageAIEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            input_type=input_type,
            truncation=truncation if truncation is not None else True,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "input_type": self.input_type,
            "truncation": self.truncation,
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
        validate_config_schema(config, "voyageai")
