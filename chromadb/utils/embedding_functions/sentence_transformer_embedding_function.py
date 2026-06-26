from chromadb.api.types import EmbeddingFunction, Space, Embeddings, Documents
from typing import List, Dict, Any, Optional
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema


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
        batch_size: Optional[int] = None,
        multiprocess_devices: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        """Initialize SentenceTransformerEmbeddingFunction.

        Args:
            model_name (str, optional): Identifier of the SentenceTransformer model,
                defaults to "all-MiniLM-L6-v2"
            device (str, optional): Device used for computation, defaults to "cpu".
                Ignored when multiprocess_devices is set.
            normalize_embeddings (bool, optional): Whether to normalize returned
                vectors, defaults to False.
            batch_size (int, optional): Number of documents to encode per batch.
                Defaults to None (uses sentence-transformers default of 32).
            multiprocess_devices (list[str], optional): If provided, encoding is
                distributed across these devices using sentence-transformers'
                built-in multi-process pool. Example: ["cuda:0", "cuda:1"] or
                ["cpu", "cpu", "cpu", "cpu"]. When set, `device` is ignored.
                Defaults to None (single-process).
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
        self.batch_size = batch_size
        self.multiprocess_devices = multiprocess_devices

        for key, value in kwargs.items():
            if not isinstance(value, (str, int, float, bool, list, dict, tuple)):
                raise ValueError(f"Keyword argument {key} is not a primitive type")
        self.kwargs = kwargs

        if model_name not in self.models:
            self.models[model_name] = SentenceTransformer(
                model_name_or_path=model_name, device=device, **kwargs
            )
        self._model = self.models[model_name]

        # Start a persistent multi-process pool if devices are specified.
        # Kept alive for the lifetime of this instance to avoid paying
        # process-spawn overhead on every __call__.
        self._pool: Optional[Dict[str, Any]] = None
        if self.multiprocess_devices is not None:
            self._pool = self._model.start_multi_process_pool(
                target_devices=self.multiprocess_devices
            )

    def __del__(self) -> None:
        if self._pool is not None:
            try:
                self._model.stop_multi_process_pool(self._pool)
            except Exception:
                pass

    def __call__(self, input: Documents) -> Embeddings:
        """Generate embeddings for the given documents.

        Args:
            input: Documents to generate embeddings for.

        Returns:
            Embeddings for the documents.
        """
        encode_kwargs: Dict[str, Any] = {
            "convert_to_numpy": True,
            "normalize_embeddings": self.normalize_embeddings,
            "pool": self._pool,
        }
        if self.batch_size is not None:
            encode_kwargs["batch_size"] = self.batch_size

        embeddings = self._model.encode(
            list(input),
            **encode_kwargs,
        )

        return [np.array(embedding, dtype=np.float32) for embedding in embeddings]

    @staticmethod
    def name() -> str:
        return "sentence_transformer"

    def default_space(self) -> Space:
        # If normalize_embeddings is True, cosine is equivalent to dot product
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        device = config.get("device")
        normalize_embeddings = config.get("normalize_embeddings")
        batch_size = config.get("batch_size")
        multiprocess_devices = config.get("multiprocess_devices", None)
        kwargs = config.get("kwargs", {})

        if model_name is None or device is None or normalize_embeddings is None:
            assert False, "This code should not be reached"

        return SentenceTransformerEmbeddingFunction(
            model_name=model_name,
            device=device,
            normalize_embeddings=normalize_embeddings,
            batch_size=batch_size,
            multiprocess_devices=multiprocess_devices,
            **kwargs,
        )

    def get_config(self) -> Dict[str, Any]:
        return {
            "model_name": self.model_name,
            "device": self.device,
            "normalize_embeddings": self.normalize_embeddings,
            "batch_size": self.batch_size,
            "multiprocess_devices": self.multiprocess_devices,
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
        validate_config_schema(config, "sentence_transformer")
