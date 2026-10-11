from chromadb.api.types import EmbeddingFunction, Space, Embeddings, Documents
from typing import List, Dict, Any, Optional
import numpy as np
from chromadb.utils.embedding_functions.schemas import validate_config_schema


class FusionEmbeddingFunction(EmbeddingFunction[Documents]):
    """Text embeddings from Eximius Labs' open-weight Fusion Embedding models
    (e.g. ``EximiusLabs/fusion-embedding-2-2b-preview``).

    Loads locally through the ``fusion-embedding`` package's text-only path (the frozen
    base plus the trained connector head, without the audio tower). Install the model
    package with::

        pip install git+https://github.com/Eximius-Labs/fusion-embedding
    """

    # Cache loaded models across instances (dynamic import -> typed Any).
    models: Dict[str, Any] = {}

    def __init__(
        self,
        model_name: str = "EximiusLabs/fusion-embedding-2-2b-preview",
        device: str = "cpu",
        dim: Optional[int] = None,
    ):
        try:
            from fusion_embedding import FusionTextEmbedder
        except ImportError:
            raise ValueError(
                "The fusion-embedding package is not installed. Install it with "
                "`pip install git+https://github.com/Eximius-Labs/fusion-embedding`"
            )

        self.model_name = model_name
        self.device = device
        self.dim = dim

        cache_key = f"{model_name}@{device}"
        if cache_key not in self.models:
            self.models[cache_key] = FusionTextEmbedder.from_pretrained(
                model_name, device=device
            )
        self._model = self.models[cache_key]

    def __call__(self, input: Documents) -> Embeddings:
        vectors = self._model.encode(list(input), dim=self.dim)  # np.ndarray [N, D]
        return [np.asarray(vector, dtype=np.float32) for vector in vectors]

    @staticmethod
    def name() -> str:
        return "fusion_embedding"

    def default_space(self) -> Space:
        return "cosine"

    def supported_spaces(self) -> List[Space]:
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")
        device = config.get("device")
        if model_name is None or device is None:
            assert False, "This code should not be reached"
        return FusionEmbeddingFunction(
            model_name=model_name, device=device, dim=config.get("dim")
        )

    def get_config(self) -> Dict[str, Any]:
        return {"model_name": self.model_name, "device": self.device, "dim": self.dim}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        validate_config_schema(config, "fusion_embedding")
