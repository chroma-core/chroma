import logging
from typing import Any, Dict, cast

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class SentenceTransformerEmbeddingFunction(EmbeddingFunction[Documents]):
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
        if model_name not in self.models:
            try:
                from sentence_transformers import SentenceTransformer
            except ImportError:
                raise ValueError(
                    "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
                )
            self.models[model_name] = SentenceTransformer(
                model_name, device=device, **kwargs
            )
        self._model = self.models[model_name]
        self._normalize_embeddings = normalize_embeddings

    def __call__(self, input: Documents) -> Embeddings:
        return cast(
            Embeddings,
            [
                embedding for embedding in self._model.encode(
                    list(input),
                    convert_to_numpy=True,
                    normalize_embeddings=self._normalize_embeddings,
                )
            ]
        )
