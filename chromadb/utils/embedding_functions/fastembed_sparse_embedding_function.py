from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseEmbeddings,
    Documents,
)
from typing import Dict, Any, TypedDict, Optional
from typing import cast, Literal
from chromadb.utils.embedding_functions.schemas import validate_config_schema

TaskType = Literal["document", "query"]


class FastembedSparseEmbeddingFunctionQueryConfig(TypedDict):
    task: TaskType


class FastembedSparseEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    def __init__(
        self,
        model_name: str,
        task: Optional[TaskType] = "document",
        query_config: Optional[FastembedSparseEmbeddingFunctionQueryConfig] = None,
    ):
        """Initialize SparseEncoderEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the Splade model
            List of commonly used models: Qdrant/bm25, prithivida/Splade_PP_en_v1, Qdrant/minicoil-v1
            task (str, optional): Task to perform, can be "document" or "query"
            query_config (dict, optional): Configuration for the query, can be "task"
        """
        try:
            from fastembed import SparseTextEmbedding
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )

        self.task = task
        self.query_config = query_config
        self.model_name = model_name
        self._model = SparseTextEmbedding(model_name)

    def __call__(self, input: Documents) -> SparseEmbeddings:
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
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
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

        sparse_embeddings: SparseEmbeddings = []

        for vec in embeddings:
            sparse_embeddings.append(
                {"indices": vec.indices.tolist(), "values": vec.values.tolist()}
            )

        return sparse_embeddings

    def embed_query(self, input: Documents) -> SparseEmbeddings:
        try:
            from fastembed import SparseTextEmbedding
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
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

            sparse_embeddings: SparseEmbeddings = []

            for vec in embeddings:
                sparse_embeddings.append(
                    {"indices": vec.indices.tolist(), "values": vec.values.tolist()}
                )

            return sparse_embeddings

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
        if model_name is None:
            assert False, "This code should not be reached"

        return FastembedSparseEmbeddingFunction(
            model_name=model_name,
            task=task,
            query_config=query_config,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "task": self.task,
            "query_config": self.query_config,
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
