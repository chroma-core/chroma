from chromadb.api.types import (
    EmbeddingFunction,
    Embeddings,
    Documents,
    Space,
)
from typing import Any, Dict, List, Literal, Optional, Sequence, TypedDict, cast
import numpy as np

from chromadb.utils.embedding_functions.schemas import validate_config_schema

TaskType = Literal["document", "query", "passage"]


class FastembedTextEmbeddingFunctionQueryConfig(TypedDict):
    task: TaskType


class FastembedTextEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(
        self,
        model_name: str = "BAAI/bge-small-en-v1.5",
        task: Optional[TaskType] = "document",
        query_config: Optional[FastembedTextEmbeddingFunctionQueryConfig] = None,
        cache_dir: Optional[str] = None,
        threads: Optional[int] = None,
        providers: Optional[Sequence[Any]] = None,
        cuda: bool = False,
        device_ids: Optional[List[int]] = None,
        lazy_load: bool = False,
        **kwargs: Any,
    ):
        """Initialize FastembedTextEmbeddingFunction."""
        try:
            from fastembed import TextEmbedding
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )

        self.model_name = model_name
        self.task = task
        self.query_config = query_config
        self.cache_dir = cache_dir
        self.threads = threads
        self.providers = providers
        self.cuda = cuda
        self.device_ids = device_ids
        self.lazy_load = lazy_load
        for key, value in kwargs.items():
            if not isinstance(value, (str, int, float, bool, list, dict, tuple)):
                raise ValueError(f"Keyword argument {key} is not a primitive type")
        self.kwargs = kwargs
        self._model = TextEmbedding(
            model_name=model_name,
            cache_dir=cache_dir,
            threads=threads,
            providers=providers,
            cuda=cuda,
            device_ids=device_ids,
            lazy_load=lazy_load,
            **kwargs,
        )

    def _embed(self, input: Documents, task: Optional[TaskType]) -> Embeddings:
        try:
            from fastembed import TextEmbedding
        except ImportError:
            raise ValueError(
                "The fastembed python package is not installed. Please install it with `pip install fastembed`"
            )

        model = cast(TextEmbedding, self._model)
        if task == "document":
            embeddings = model.embed(list(input))
        elif task == "query":
            embeddings = model.query_embed(list(input))
        elif task == "passage":
            embeddings = model.passage_embed(list(input))
        else:
            raise ValueError(f"Invalid task: {task}")

        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    def __call__(self, input: Documents) -> Embeddings:
        """Generate embeddings for the given documents."""
        return self._embed(input, self.task)

    def embed_query(self, input: Documents) -> Embeddings:
        if self.query_config is not None:
            return self._embed(input, self.query_config.get("task"))

        return self.__call__(input)

    @staticmethod
    def name() -> str:
        return "fastembed_text"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        task = config.get("task", "document")
        query_config = config.get("query_config")
        cache_dir = config.get("cache_dir")
        threads = config.get("threads")
        providers = config.get("providers")
        cuda = config.get("cuda", False)
        device_ids = config.get("device_ids")
        lazy_load = config.get("lazy_load", False)
        kwargs = config.get("kwargs", {})

        if model_name is None:
            assert False, "This code should not be reached"

        return FastembedTextEmbeddingFunction(
            model_name=model_name,
            task=task,
            query_config=query_config,
            cache_dir=cache_dir,
            threads=threads,
            providers=providers,
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
            "providers": self.providers,
            "cuda": self.cuda,
            "device_ids": self.device_ids,
            "lazy_load": self.lazy_load,
            "kwargs": self.kwargs,
        }

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "fastembed_text")
