from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseEmbeddings,
    Documents,
)
from typing import Dict, Any, TypedDict, Optional
from typing import cast, Literal
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.sparse_embedding_utils import _sort_sparse_vectors

TaskType = Literal["document", "query"]


class Bm25EmbeddingFunctionQueryConfig(TypedDict):
    task: TaskType


class Bm25EmbeddingFunction(SparseEmbeddingFunction[Documents]):
    def __init__(
        self,
        avg_len: Optional[float] = None,
        task: Optional[TaskType] = "document",
        cache_dir: Optional[str] = None,
        k: Optional[float] = None,
        b: Optional[float] = None,
        language: Optional[str] = None,
        token_max_length: Optional[int] = None,
        disable_stemmer: Optional[bool] = None,
        specific_model_path: Optional[str] = None,
        query_config: Optional[Bm25EmbeddingFunctionQueryConfig] = None,
        **kwargs: Any,
    ):
        """Initialize SparseEncoderEmbeddingFunction.

        Args:
            avg_len(float, optional): The average length of the documents in the corpus.
            task (str, optional): Task to perform, can be "document" or "query"
            cache_dir (str, optional): The path to the cache directory.
            k (float, optional): The k parameter in the BM25 formula. Defines the saturation of the term frequency.
            b (float, optional): The b parameter in the BM25 formula. Defines the importance of the document length.
            language (str, optional): Specifies the language for the stemmer.
            token_max_length (int, optional): The maximum length of the tokens.
            disable_stemmer (bool, optional): Disable the stemmer.
            specific_model_path (str, optional): The path to the specific model.
            query_config (dict, optional): Configuration for the query, can be "task"
            **kwargs: Additional arguments to pass to the Bm25 model.
        """
        try:
            from fastembed.sparse.bm25 import Bm25
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )

        self.task = task
        self.query_config = query_config
        self.cache_dir = cache_dir
        self.k = k
        self.b = b
        self.avg_len = avg_len
        self.language = language
        self.token_max_length = token_max_length
        self.disable_stemmer = disable_stemmer
        self.specific_model_path = specific_model_path
        for key, value in kwargs.items():
            if not isinstance(value, (str, int, float, bool, list, dict, tuple)):
                raise ValueError(f"Keyword argument {key} is not a primitive type")
        self.kwargs = kwargs
        bm25_kwargs = {
            "model_name": "Qdrant/bm25",
        }
        optional_params = {
            "cache_dir": cache_dir,
            "k": k,
            "b": b,
            "avg_len": avg_len,
            "language": language,
            "token_max_length": token_max_length,
            "disable_stemmer": disable_stemmer,
            "specific_model_path": specific_model_path,
        }
        for key, value in optional_params.items():
            if value is not None:
                bm25_kwargs[key] = value
        bm25_kwargs.update({k: v for k, v in kwargs.items() if v is not None})
        self._model = Bm25(**bm25_kwargs)

    def __call__(self, input: Documents) -> SparseEmbeddings:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        try:
            from fastembed.sparse.bm25 import Bm25
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )
        model = cast(Bm25, self._model)
        if self.task == "document":
            embeddings = model.embed(
                list(input),
            )
        elif self.task == "query":
            embeddings = model.query_embed(
                list(input),
            )
        else:
            raise ValueError(f"Invalid task: {self.task}")

        sparse_embeddings: SparseEmbeddings = []

        for vec in embeddings:
            sparse_embeddings.append(
                {"indices": vec.indices.tolist(), "values": vec.values.tolist()}
            )

        _sort_sparse_vectors(sparse_embeddings)
        return sparse_embeddings

    def embed_query(self, input: Documents) -> SparseEmbeddings:
        try:
            from fastembed.sparse.bm25 import Bm25
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )
        model = cast(Bm25, self._model)
        if self.query_config is not None:
            task = self.query_config.get("task")
            if task == "document":
                embeddings = model.embed(
                    list(input),
                )
            elif task == "query":
                embeddings = model.query_embed(
                    list(input),
                )
            else:
                raise ValueError(f"Invalid task: {task}")

            sparse_embeddings: SparseEmbeddings = []

            for vec in embeddings:
                sparse_embeddings.append(
                    {"indices": vec.indices.tolist(), "values": vec.values.tolist()}
                )

            _sort_sparse_vectors(sparse_embeddings)
            return sparse_embeddings

        else:
            return self.__call__(input)

    @staticmethod
    def name() -> str:
        return "bm25"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        task = config.get("task")
        query_config = config.get("query_config")
        cache_dir = config.get("cache_dir")
        k = config.get("k")
        b = config.get("b")
        avg_len = config.get("avg_len")
        language = config.get("language")
        token_max_length = config.get("token_max_length")
        disable_stemmer = config.get("disable_stemmer")
        specific_model_path = config.get("specific_model_path")
        kwargs = config.get("kwargs", {})

        return Bm25EmbeddingFunction(
            task=task,
            query_config=query_config,
            cache_dir=cache_dir,
            k=k,
            b=b,
            avg_len=avg_len,
            language=language,
            token_max_length=token_max_length,
            disable_stemmer=disable_stemmer,
            specific_model_path=specific_model_path,
            **kwargs,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "task": self.task,
            "query_config": self.query_config,
            "cache_dir": self.cache_dir,
            "k": self.k,
            "b": self.b,
            "avg_len": self.avg_len,
            "language": self.language,
            "token_max_length": self.token_max_length,
            "disable_stemmer": self.disable_stemmer,
            "specific_model_path": self.specific_model_path,
            "kwargs": self.kwargs,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        # Users should be able to change the path if needed, so we should not validate that.
        # e.g. moving file path from /v1/my-model.bin to /v2/my-model.bin
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "bm25")
