from typing import Any, Optional, cast

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings


class FastEmbedEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to generate embeddings for a list of texts using FastEmbed - https://qdrant.github.io/fastembed/.
    Find the list of supported models at https://qdrant.github.io/fastembed/examples/Supported_Models/.
    """

    def __init__(
        self,
        model_name: str = "BAAI/bge-small-en-v1.5",
        cache_dir: Optional[str] = None,
        threads: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize fastembed.TextEmbedding

        Args:
            model_name (str): The name of the model to use.
            cache_dir (str, optional): The path to the model cache directory.
                                       Can also be set using the `FASTEMBED_CACHE_PATH` env variable.
            threads (int, optional): The number of threads single onnxruntime session can use..

        Raises:
            ValueError: If the model_name is not in the format <org>/<model> e.g. BAAI/bge-base-en.
        """
        try:
            from fastembed import TextEmbedding
        except ImportError:
            raise ValueError(
                "The 'fastembed' package is not installed. Please install it with `pip install fastembed`"
            )
        self._model = TextEmbedding(
            model_name=model_name, cache_dir=cache_dir, threads=threads, **kwargs
        )

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.

        Example:
            >>> fastembed_ef = FastEmbedEmbeddingFunction(model_name="sentence-transformers/all-MiniLM-L6-v2")
            >>> texts = ["Hello, world!", "How are you?"]
            >>> embeddings = fastembed_ef(texts)
        """
        embeddings = self._model.embed(input)
        return cast(
            Embeddings,
            [embedding.tolist() for embedding in embeddings],
        )
