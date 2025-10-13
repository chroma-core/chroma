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
from typing import List, Dict, Any, Optional, Union, Generator, Callable
import os
import numpy as np
import warnings
import importlib

# Token limits for different VoyageAI models
VOYAGE_TOTAL_TOKEN_LIMITS = {
    "voyage-context-3": 32_000,
    "voyage-3.5-lite": 1_000_000,
    "voyage-3.5": 320_000,
    "voyage-2": 320_000,
    "voyage-3-large": 120_000,
    "voyage-code-3": 120_000,
    "voyage-large-2-instruct": 120_000,
    "voyage-finance-2": 120_000,
    "voyage-multilingual-2": 120_000,
    "voyage-law-2": 120_000,
    "voyage-large-2": 120_000,
    "voyage-3": 120_000,
    "voyage-3-lite": 120_000,
    "voyage-code-2": 120_000,
    "voyage-3-m-exp": 120_000,
    "voyage-multimodal-3": 120_000,
}


class VoyageAIEmbeddingFunction(EmbeddingFunction[Embeddable]):
    """
    This class is used to generate embeddings for a list of texts using the VoyageAI API.
    """

    def __init__(
        self,
        model_name: str,
        api_key: Optional[str] = None,
        api_key_env_var: str = "CHROMA_VOYAGE_API_KEY",
        input_type: Optional[str] = None,
        truncation: bool = True,
        dimensions: Optional[int] = None,
        embedding_type: Optional[str] = None,
        batch_size: Optional[int] = None,
    ):
        """
        Initialize the VoyageAIEmbeddingFunction.

        Args:
            model_name (str): The name of the model to use for text embeddings.
            api_key_env_var (str, optional): Environment variable name that contains your API key for the VoyageAI API.
                Defaults to "CHROMA_VOYAGE_API_KEY".
            api_key (str, optional): API key for the VoyageAI API. If not provided, will look for it in the environment variable.
            input_type (str, optional): The type of input to use for the VoyageAI API.
                Defaults to None.
            truncation (bool): Whether to truncate the input text.
                Defaults to True.
            dimensions (int, optional): The output dimension for embeddings.
                Defaults to None.
            embedding_type (str, optional): The embedding type.
                Defaults to None.
            batch_size (int, optional): Maximum number of texts to embed in a single batch.
                Defaults to None (no limit).
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

        # Validate model_name
        if not model_name or not model_name.strip():
            raise ValueError("model_name cannot be None or empty")

        self.model_name = model_name
        self.input_type = input_type
        self.truncation = truncation
        self.dimensions = dimensions
        self.embedding_type = embedding_type
        self.batch_size = batch_size
        self._client = voyageai.Client(api_key=self.api_key)

    def __call__(self, input: Embeddable) -> Embeddings:
        """
        Generate embeddings for the given documents or images.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents or images.
        """
        # Early return for empty input
        if not input:
            return []

        # Handle multimodal mixed inputs (images and text) separately - no batching
        if self._is_multimodal_model() and not all(isinstance(i, str) for i in input):
            embeddings = self._client.multimodal_embed(
                inputs=[[self.convert(i)] for i in input],
                model=self.model_name,
                input_type=self.input_type,
                truncation=self.truncation,
            ).embeddings
        else:
            # Use unified batching for all text inputs (regular, context, multimodal text-only)
            # Cast to List[str] since we know all inputs are strings at this point
            embeddings = self._embed_with_batching(list(input))  # type: ignore[arg-type]

        # Convert to numpy arrays
        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    def _build_batches(self, texts: List[str]) -> Generator[List[str], None, None]:
        """
        Generate batches of texts based on token limits using a generator.

        Args:
            texts: List of texts to batch.

        Yields:
            Batches of texts as lists.
        """
        if not texts:
            return

        max_tokens_per_batch = self.get_token_limit()
        current_batch: List[str] = []
        current_batch_tokens = 0

        # Tokenize all texts in one API call
        all_token_lists = self._client.tokenize(texts, model=self.model_name)
        token_counts = [len(tokens) for tokens in all_token_lists]

        for i, text in enumerate(texts):
            n_tokens = token_counts[i]

            # Check if adding this text would exceed limits
            if current_batch and (
                (self.batch_size and len(current_batch) >= self.batch_size)
                or (current_batch_tokens + n_tokens > max_tokens_per_batch)
            ):
                # Yield the current batch and start a new one
                yield current_batch
                current_batch = []
                current_batch_tokens = 0

            current_batch.append(text)
            current_batch_tokens += n_tokens

        # Yield the last batch (always has at least one text)
        yield current_batch

    def _get_embed_function(self) -> Callable[[List[str]], List[List[float]]]:
        """
        Get the appropriate embedding function based on model type.

        Returns:
            A callable that takes a batch of texts and returns embeddings.
        """
        if self._is_context_model():

            def embed_batch(batch: List[str]) -> List[List[float]]:
                result = self._client.contextualized_embed(
                    inputs=[batch],
                    model=self.model_name,
                    input_type=self.input_type,
                    output_dimension=self.dimensions,
                )
                return list(result.results[0].embeddings)

            return embed_batch

        elif self._is_multimodal_model():

            def embed_batch(batch: List[str]) -> List[List[float]]:
                result = self._client.multimodal_embed(
                    inputs=[[text] for text in batch],
                    model=self.model_name,
                    input_type=self.input_type,
                    truncation=self.truncation,
                )
                return list(result.embeddings)

            return embed_batch

        else:

            def embed_batch(batch: List[str]) -> List[List[float]]:
                result = self._client.embed(
                    texts=batch,
                    model=self.model_name,
                    input_type=self.input_type,
                    truncation=self.truncation,
                    output_dimension=self.dimensions,
                )
                return list(result.embeddings)

            return embed_batch

    def _embed_with_batching(self, texts: List[str]) -> List[List[float]]:
        """
        Unified method to embed texts with automatic batching based on token limits.
        Works for regular, contextual, and multimodal (text-only) models.

        Args:
            texts: List of texts to embed.

        Returns:
            List of embeddings.
        """
        if not texts:
            return []

        # Get the appropriate embedding function for this model type
        embed_fn = self._get_embed_function()

        # Process each batch
        all_embeddings = []
        for batch in self._build_batches(texts):
            batch_embeddings = embed_fn(batch)
            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    def count_tokens(self, texts: List[str]) -> List[int]:
        """
        Count tokens for the given texts.

        Args:
            texts: List of texts to count tokens for.

        Returns:
            List of token counts for each text.
        """
        if not texts:
            return []

        # Use the VoyageAI tokenize API to get token counts
        token_lists = self._client.tokenize(texts, model=self.model_name)
        return [len(token_list) for token_list in token_lists]

    def get_token_limit(self) -> int:
        """
        Get the token limit for the current model.

        Returns:
            Token limit for the model, or default of 120_000 if not found.
        """
        return VOYAGE_TOTAL_TOKEN_LIMITS.get(self.model_name, 120_000)

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
            raise ValueError(
                f"Unsupported input type: {type(embeddable)}. "
                "Expected Document (str) or Image (numpy array)"
            )

    def _is_context_model(self) -> bool:
        """Check if the model is a contextualized embedding model."""
        return "context" in self.model_name

    def _is_multimodal_model(self) -> bool:
        """Check if the model is a multimodal embedding model."""
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
        dimensions = config.get("dimensions")
        embedding_type = config.get("embedding_type")
        batch_size = config.get("batch_size")

        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return VoyageAIEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            input_type=input_type,
            truncation=truncation if truncation is not None else True,
            dimensions=dimensions,
            embedding_type=embedding_type,
            batch_size=batch_size,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
            "input_type": self.input_type,
            "truncation": self.truncation,
            "dimensions": self.dimensions,
            "embedding_type": self.embedding_type,
            "batch_size": self.batch_size,
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
