from chromadb.api.types import (
    SparseEmbeddingFunction,
    SparseVectors,
    Documents,
)
from typing import Dict, Any, TypedDict, Optional
import numpy as np
from typing import cast, Literal
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.sparse_embedding_utils import normalize_sparse_vector

TaskType = Literal["document", "query"]


class HuggingFaceSparseEmbeddingFunctionQueryConfig(TypedDict):
    task: TaskType


class HuggingFaceSparseEmbeddingFunction(SparseEmbeddingFunction[Documents]):
    # Since we do dynamic imports we have to type this as Any
    models: Dict[str, Any] = {}

    def __init__(
        self,
        model_name: str,
        device: str,
        task: Optional[TaskType] = "document",
        query_config: Optional[HuggingFaceSparseEmbeddingFunctionQueryConfig] = None,
        **kwargs: Any,
    ):
        """Initialize SparseEncoderEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the Huggingface SparseEncoder model
            Some common models: prithivida/Splade_PP_en_v1, naver/splade-cocondenser-ensembledistil, naver/splade-v3
            device (str, optional): Device used for computation
            **kwargs: Additional arguments to pass to the Splade model.
        """
        try:
            from sentence_transformers import SparseEncoder
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )

        self.model_name = model_name
        self.device = device
        self.task = task
        self.query_config = query_config
        for key, value in kwargs.items():
            if not isinstance(value, (str, int, float, bool, list, dict, tuple)):
                raise ValueError(f"Keyword argument {key} is not a primitive type")
        self.kwargs = kwargs

        if model_name not in self.models:
            self.models[model_name] = SparseEncoder(
                model_name_or_path=model_name, device=device, **kwargs
            )
        self._model = self.models[model_name]

    def __call__(self, input: Documents) -> SparseVectors:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        try:
            from sentence_transformers import SparseEncoder
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )
        model = cast(SparseEncoder, self._model)
        if self.task == "document":
            embeddings = model.encode_document(
                list(input),
            )
        elif self.task == "query":
            embeddings = model.encode_query(
                list(input),
            )
        else:
            raise ValueError(f"Invalid task: {self.task}")

        sparse_vectors: SparseVectors = []

        for vec in embeddings:
            # Convert sparse tensor to dense array if needed
            if hasattr(vec, "to_dense"):
                vec_dense = vec.to_dense().numpy()
            else:
                vec_dense = vec.numpy() if hasattr(vec, "numpy") else np.array(vec)

            nz = np.where(vec_dense != 0)[0]
            sparse_vectors.append(
                normalize_sparse_vector(
                    indices=nz.tolist(), values=vec_dense[nz].tolist()
                )
            )

        return sparse_vectors

    def embed_query(self, input: Documents) -> SparseVectors:
        try:
            from sentence_transformers import SparseEncoder
        except ImportError:
            raise ValueError(
                "The sentence_transformers python package is not installed. Please install it with `pip install sentence_transformers`"
            )
        model = cast(SparseEncoder, self._model)
        if self.query_config is not None:
            if self.query_config.get("task") == "document":
                embeddings = model.encode_document(
                    list(input),
                )
            elif self.query_config.get("task") == "query":
                embeddings = model.encode_query(
                    list(input),
                )
            else:
                raise ValueError(f"Invalid task: {self.query_config.get('task')}")

            sparse_vectors: SparseVectors = []

            for vec in embeddings:
                # Convert sparse tensor to dense array if needed
                if hasattr(vec, "to_dense"):
                    vec_dense = vec.to_dense().numpy()
                else:
                    vec_dense = vec.numpy() if hasattr(vec, "numpy") else np.array(vec)

                nz = np.where(vec_dense != 0)[0]
                sparse_vectors.append(
                    normalize_sparse_vector(
                        indices=nz.tolist(), values=vec_dense[nz].tolist()
                    )
                )

            return sparse_vectors

        else:
            return self.__call__(input)

    @staticmethod
    def name() -> str:
        return "huggingface_sparse"

    @staticmethod
    def build_from_config(
        config: Dict[str, Any]
    ) -> "SparseEmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        device = config.get("device")
        task = config.get("task")
        query_config = config.get("query_config")
        kwargs = config.get("kwargs", {})

        if model_name is None or device is None:
            assert False, "This code should not be reached"

        return HuggingFaceSparseEmbeddingFunction(
            model_name=model_name,
            device=device,
            task=task,
            query_config=query_config,
            **kwargs,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "device": self.device,
            "task": self.task,
            "query_config": self.query_config,
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
        validate_config_schema(config, "huggingface_sparse")
