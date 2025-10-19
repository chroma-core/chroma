from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseVectors,
    Documents,
)
from typing import Dict, Any, TypedDict, Optional
from typing import cast, Literal
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector

TaskType = Literal["document", "query"]


class FastembedSparseEmbeddingFunctionQueryConfig(TypedDict):
    task: TaskType


class FastembedSparseEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    def __init__(
        self,
        model_name: str,
        task: Optional[TaskType] = "document",
        cache_dir: Optional[str] = None,
        threads: Optional[int] = None,
        cuda: Optional[bool] = None,
        device_ids: Optional[list[int]] = None,
        lazy_load: Optional[bool] = None,
        query_config: Optional[FastembedSparseEmbeddingFunctionQueryConfig] = None,
        **kwargs: Any,
    ):
        """Initialize SparseEncoderEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the Fastembed model
            List of commonly used models: Qdrant/bm25, prithivida/Splade_PP_en_v1, Qdrant/minicoil-v1
            task (str, optional): Task to perform, can be "document" or "query"
            cache_dir (str, optional): The path to the cache directory.
            threads (int, optional): The number of threads to use for the model.
            cuda (bool, optional): Whether to use CUDA.
            device_ids (list[int], optional): The device IDs to use for the model.
            lazy_load (bool, optional): Whether to lazy load the model.
            query_config (dict, optional): Configuration for the query, can be "task"
            **kwargs: Additional arguments to pass to the model.
        """
        try:
            from fastembed import SparseTextEmbedding
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )

        self.task = task
        self.query_config = query_config
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.threads = threads
        self.cuda = cuda
        self.device_ids = device_ids
        self.lazy_load = lazy_load
        for key, value in kwargs.items():
            if not isinstance(value, (str, int, float, bool, list, dict, tuple)):
                raise ValueError(f"Keyword argument {key} is not a primitive type")
        self.kwargs = kwargs
        self._model = SparseTextEmbedding(
            model_name, cache_dir, threads, cuda, device_ids, lazy_load, **kwargs
        )

    def __call__(self, input: Documents) -> SparseVectors:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        try:
            from fastembed import SparseTextEmbedding
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )
        model = cast(SparseTextEmbedding, self._model)
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

        sparse_vectors: SparseVectors = []

        for vec in embeddings:
            sparse_vectors.append(
                normalize_sparse_vector(
                    indices=vec.indices.tolist(), values=vec.values.tolist()
                )
            )

        return sparse_vectors

    def embed_query(self, input: Documents) -> SparseVectors:
        try:
            from fastembed import SparseTextEmbedding
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )
        model = cast(SparseTextEmbedding, self._model)
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

            sparse_vectors: SparseVectors = []

            for vec in embeddings:
                sparse_vectors.append(
                    normalize_sparse_vector(
                        indices=vec.indices.tolist(), values=vec.values.tolist()
                    )
                )

            return sparse_vectors

        else:
            return self.__call__(input)

    @staticmethod
    def name() -> str:
        return "fastembed_sparse"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        task = config.get("task")
        query_config = config.get("query_config")
        cache_dir = config.get("cache_dir")
        threads = config.get("threads")
        cuda = config.get("cuda")
        device_ids = config.get("device_ids")
        lazy_load = config.get("lazy_load")
        kwargs = config.get("kwargs", {})
        if model_name is None:
            assert False, "This code should not be reached"

        return FastembedSparseEmbeddingFunction(
            model_name=model_name,
            task=task,
            query_config=query_config,
            cache_dir=cache_dir,
            threads=threads,
            cuda=cuda,
            device_ids=device_ids,
            lazy_load=lazy_load,
            **kwargs,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "task": self.task,
            "query_config": self.query_config,
            "cache_dir": self.cache_dir,
            "threads": self.threads,
            "cuda": self.cuda,
            "device_ids": self.device_ids,
            "lazy_load": self.lazy_load,
            "kwargs": self.kwargs,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        # model_name is also used as the identifier for model path if stored locally.
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
        validate_config_schema(config, "fastembed_sparse")
