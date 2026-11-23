from chromadb.api.types import (
    Embeddings,
    Embeddable,
    EmbeddingFunction,
    Space,
    is_image,
    is_document,
)
from typing import List, Dict, Any, Optional
import os
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema
import base64
import io
import importlib
import warnings


# Models that share one embedding space. Documents embedded with one can be
# queried with another, e.g. index with Pro and query with Fast.
_EMBED_V5_MODELS = {"embed-v5.0-pro", "embed-v5.0-fast"}


class CohereEmbeddingFunction(EmbeddingFunction[Embeddable]):
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: str = "embed-v5.0-pro",
        api_key_env_var: str = "CHROMA_COHERE_API_KEY",
        query_model_name: Optional[str] = None,
        output_dimension: Optional[int] = None,
    ):
        """
        Initialize the CohereEmbeddingFunction.

        Args:
            api_key (str, optional): Cohere API key. Not persisted; prefer
                api_key_env_var.
            model_name (str, optional): Model used to embed documents and, unless
                query_model_name is set, queries. Defaults to "embed-v5.0-pro".
            api_key_env_var (str, optional): Environment variable holding the
                API key. Defaults to "CHROMA_COHERE_API_KEY"; "COHERE_API_KEY"
                is used instead when it is set.
            query_model_name (str, optional): Model used for queries. Only
                models that share an embedding space with model_name are
                allowed (currently embed-v5.0-pro and embed-v5.0-fast).
            output_dimension (int, optional): Embedding size for models that
                support Matryoshka output (embed-v4.0 and newer). Uses the
                model default when not set.
        """
        try:
            import cohere
        except ImportError:
            raise ValueError(
                "The cohere python package is not installed. Please install it with `pip install cohere`"
            )

        if not hasattr(cohere, "ClientV2"):
            raise ValueError(
                "The installed cohere python package is too old. Please upgrade it with `pip install -U cohere`"
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
        if os.getenv("COHERE_API_KEY") is not None:
            self.api_key_env_var = "COHERE_API_KEY"
        else:
            self.api_key_env_var = api_key_env_var

        self.api_key = api_key or os.getenv(self.api_key_env_var)
        if not self.api_key:
            raise ValueError(
                f"The {self.api_key_env_var} environment variable is not set."
            )

        if query_model_name is not None and query_model_name != model_name:
            if not (
                model_name in _EMBED_V5_MODELS and query_model_name in _EMBED_V5_MODELS
            ):
                raise ValueError(
                    "query_model_name must share an embedding space with model_name. "
                    f"Supported pairs: {sorted(_EMBED_V5_MODELS)}."
                )

        self.model_name = model_name
        self.query_model_name = query_model_name
        self.output_dimension = output_dimension

        self.client = cohere.ClientV2(api_key=self.api_key)

    def _embed_texts(
        self, texts: List[str], model: str, input_type: str
    ) -> Embeddings:
        kwargs: Dict[str, Any] = {}
        if self.output_dimension is not None:
            kwargs["output_dimension"] = self.output_dimension
        response = self.client.embed(
            texts=texts,
            model=model,
            input_type=input_type,
            embedding_types=["float"],
            **kwargs,
        )
        return [
            np.array(embedding, dtype=np.float32)
            for embedding in response.embeddings.float_
        ]

    def _embed_images(self, images: List[Any], model: str) -> Embeddings:
        base64_images = []
        for image_np in images:
            if not isinstance(image_np, np.ndarray):
                raise ValueError(
                    f"Expected image input to be a numpy array, got {type(image_np)}"
                )

            try:
                pil_image = self._PILImage.fromarray(image_np)

                buffer = io.BytesIO()
                pil_image.save(buffer, format="PNG")
                img_bytes = buffer.getvalue()

                # Encode bytes to base64 string
                base64_string = base64.b64encode(img_bytes).decode("utf-8")

                data_uri = f"data:image/png;base64,{base64_string}"
                base64_images.append(data_uri)

            except Exception as e:
                raise ValueError(
                    f"Failed to convert image numpy array to base64 data URI: {e}"
                ) from e

        kwargs: Dict[str, Any] = {}
        if self.output_dimension is not None:
            kwargs["output_dimension"] = self.output_dimension
        response = self.client.embed(
            images=base64_images,
            model=model,
            input_type="image",
            embedding_types=["float"],
            **kwargs,
        )
        return [
            np.array(embedding, dtype=np.float32)
            for embedding in response.embeddings.float_
        ]

    def _embed(self, input: Embeddable, model: str, text_input_type: str) -> Embeddings:
        # Cohere works with images. if all are texts, return the embeddings for the texts
        if all(is_document(item) for item in input):
            return self._embed_texts(
                [str(item) for item in input], model, text_input_type
            )
        elif all(is_image(item) for item in input):
            return self._embed_images(list(input), model)
        else:
            # Check if it's a mix or neither
            has_texts = any(is_document(item) for item in input)
            has_images = any(is_image(item) for item in input)
            if has_texts and has_images:
                raise ValueError(
                    "Input contains a mix of text documents and images, which is not supported. Provide either all texts or all images."
                )
            else:
                raise ValueError(
                    "Input must be a list of text documents (str) or a list of images (numpy arrays)."
                )

    def __call__(self, input: Embeddable) -> Embeddings:
        """
        Generate embeddings for the given documents.

        Args:
            input: Documents or images to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        return self._embed(input, self.model_name, "search_document")

    def embed_query(self, input: Embeddable) -> Embeddings:
        """
        Generate embeddings for the given queries.

        Text queries are embedded with input_type="search_query", which is
        what Cohere's retrieval models expect at query time.

        Args:
            input: Query texts or images to generate embeddings for.

        Returns:
            Embeddings for the queries.
        """
        return self._embed(
            input, self.query_model_name or self.model_name, "search_query"
        )

    @staticmethod
    def name() -> str:
        return "cohere"

    def default_space(self) -> Space:
        if self.model_name == "embed-multilingual-v2.0":
            return "ip"
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        if self.model_name == "embed-english-v2.0":
            return ["cosine"]
        elif self.model_name == "embed-english-light-v2.0":
            return ["cosine"]
        elif self.model_name == "embed-multilingual-v2.0":
            return ["ip"]
        else:
            return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Embeddable]":
        api_key_env_var = config.get("api_key_env_var")
        model_name = config.get("model_name")
        if api_key_env_var is None or model_name is None:
            assert False, "This code should not be reached"

        return CohereEmbeddingFunction(
            api_key_env_var=api_key_env_var,
            model_name=model_name,
            query_model_name=config.get("query_model_name"),
            output_dimension=config.get("output_dimension"),
        )

    def get_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            "api_key_env_var": self.api_key_env_var,
            "model_name": self.model_name,
        }
        # Only write the new keys when set, so configs for existing
        # collections are unchanged.
        if self.query_model_name is not None:
            config["query_model_name"] = self.query_model_name
        if self.output_dimension is not None:
            config["output_dimension"] = self.output_dimension
        return config

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )
        if "output_dimension" in new_config:
            raise ValueError(
                "The output dimension cannot be changed after the embedding function has been initialized."
            )
        if "query_model_name" in new_config:
            query_model_name = new_config["query_model_name"]
            model_name = old_config.get("model_name")
            if query_model_name not in (None, model_name) and not (
                model_name in _EMBED_V5_MODELS and query_model_name in _EMBED_V5_MODELS
            ):
                raise ValueError(
                    "query_model_name must share an embedding space with model_name."
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
        validate_config_schema(config, "cohere")
