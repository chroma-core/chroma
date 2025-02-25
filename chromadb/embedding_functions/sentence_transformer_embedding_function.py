from chromadb.embedding_functions.embedding_function import EmbeddingFunction, Space
from chromadb.api.types import Embeddings, Embeddable
from typing import List, Dict, Any
import numpy as np


class SentenceTransformerEmbeddingFunction(EmbeddingFunction[Embeddable]):
    # Since we do dynamic imports we have to type this as Any
    models: Dict[str, Any] = {}

    # If you have a beefier machine, try "gtr-t5-large".
    # for a full list of options: https://huggingface.co/sentence-transformers, https://www.sbert.net/docs/pretrained_models.html
    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        device: str = "cpu",
        normalize_embeddings: bool = False,
        **kwargs: Any,
    ):
        """Initialize SentenceTransformerEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the SentenceTransformer model, defaults to "all-MiniLM-L6-v2"
            device (str, optional): Device used for computation, defaults to "cpu"
            normalize_embeddings (bool, optional): Whether to normalize returned vectors, defaults to False
            **kwargs: Additional arguments to pass to the SentenceTransformer model.
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )

        self.model_name = model_name
        self.device = device
        self.normalize_embeddings = normalize_embeddings
        self.kwargs = kwargs

        if model_name not in self.models:
            self.models[model_name] = SentenceTransformer(
                model_name, device=device, **kwargs
            )
        self._model = self.models[model_name]

    def __call__(self, input: Embeddable) -> Embeddings:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        embeddings = self._model.encode(
            list(input),
            convert_to_numpy=True,
            normalize_embeddings=self.normalize_embeddings,
        )

        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    def name(self) -> str:
        return "sentence_transformer"

    def default_space(self) -> Space:
        # If normalize_embeddings is True, cosine is equivalent to dot product
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

    def max_tokens(self) -> int:
        # Default token limit for SentenceTransformer models
        # This is a conservative estimate, actual limits vary by model
        return 512

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Embeddable]":
        model_name = config.get("model_name", "all-MiniLM-L6-v2")
        device = config.get("device", "cpu")
        normalize_embeddings = config.get("normalize_embeddings", False)
        kwargs = config.get("kwargs", {})

        return SentenceTransformerEmbeddingFunction(
            model_name=model_name,
            device=device,
            normalize_embeddings=normalize_embeddings,
            **kwargs,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "device": self.device,
            "normalize_embeddings": self.normalize_embeddings,
            "kwargs": self.kwargs,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )

    def validate_config(self, config: Dict[str, Any]) -> None:
        # TODO: Validate with JSON schema
        pass
