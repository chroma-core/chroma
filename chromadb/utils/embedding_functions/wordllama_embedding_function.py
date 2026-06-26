from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Literal, cast

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

if TYPE_CHECKING:
    from wordllama.inference import WordLlamaInference


class WordLlamaEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    WordLlama is a fast, lightweight NLP toolkit designed for tasks like fuzzy deduplication, similarity computation,
    ranking, clustering, and semantic text splitting. It operates with minimal inference-time dependencies
    and is optimized for CPU hardware, making it suitable for deployment in resource-constrained environments.

    https://github.com/dleemiller/WordLlama
    """
    models: Dict[str, WordLlamaInference] = {}

    def __init__(
        self,
        config: Literal["l2_supercat", "l3_supercat"] = "l2_supercat",
        normalize_embeddings: bool = False,
        **kwargs: Any,
    ):
        """Initialize WordLlamaEmbeddingFunction.

        Args:
            config (Literal["l2_supercat", "l3_supercat"]): Identifier of the WordLlama config, defaults to "l2_supercat".
            normalize_embeddings (bool, optional): Whether to normalize returned vectors, defaults to False
            **kwargs: Additional arguments to pass to the WordLlama.load function.
        """
        if config not in self.models:
            try:
                from wordllama import WordLlama
            except ImportError:
                raise ValueError(
                    "The wordllama python package is not installed. Please install it with `pip install wordllama`"
                )
            self.models[config] = WordLlama.load(config, **kwargs)
        self._model = self.models[config]
        self._normalize_embeddings = normalize_embeddings

    def __call__(self, input: Documents) -> Embeddings:
        return cast(
            Embeddings,
            self._model.embed(
                list(input),
                return_np=True,
                norm=self._normalize_embeddings,
            ),
        )
