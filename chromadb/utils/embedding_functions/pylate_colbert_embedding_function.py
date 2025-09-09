from chromadb.api.types import Embeddings, Documents, EmbeddingFunction, Space
from typing import List, Dict, Any
from chromadb.utils.embedding_functions.schemas import validate_config_schema
from chromadb.utils.muvera import create_fdes


class PylateColBERTEmbeddingFunction(EmbeddingFunction[Documents]):
    """
    This class is used to get embeddings for a list of texts using the ColBERT API.
    """

    def __init__(
        self,
        model_name: str,
    ):
        """
        Initialize the PylateColBERTEmbeddingFunction.

        Args:
            model_name (str): The name of the model to use for text embeddings.
            Examples: "mixedbread-ai/mxbai-edge-colbert-v0-17m", "mixedbread-ai/mxbai-edge-colbert-v0-32m", "lightonai/colbertv2.0", "answerdotai/answerai-colbert-small-v1", "jinaai/jina-colbert-v2", "GTE-ModernColBERT-v1"
        """
        try:
            from pylate import models
        except ImportError:
            raise ValueError(
                "The pylate colbert python package is not installed. Please install it with `pip install pylate-colbert`"
            )

        self.model_name = model_name
        self.model = models.ColBERT(model_name_or_path=model_name)

    def __call__(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.
        """
        multivec = self.model.encode(input, batch_size=32, is_query=False)
        if not multivec or not multivec[0]:
            raise ValueError("Model returned empty multivector embeddings")
        return create_fdes(
            multivec,
            dims=len(multivec[0][0]),
            is_query=False,
            fill_empty_partitions=True,
        )

    def embed_query(self, input: Documents) -> Embeddings:
        """
        Get the embeddings for a list of texts.

        Args:
            input (Documents): A list of texts to get embeddings for.

        Returns:
            Embeddings: The embeddings for the texts.
        """
        multivec = self.model.encode(input, batch_size=32, is_query=True)
        if not multivec or not multivec[0]:
            raise ValueError("Model returned empty multivector embeddings")
        return create_fdes(
            multivec,
            dims=len(multivec[0][0]),
            is_query=True,
            fill_empty_partitions=False,
        )

    @staticmethod
    def name() -> str:
        return "pylate_colbert"

    def default_space(self) -> Space:
        return "ip"  # muvera uses dot product to approximate multivec similarity

    def supported_spaces(self) -> List[Space]:
        return [
            "ip"
        ]  # no cosine bc muvera does not normalize the fde, no l2 bc muvera uses dot product

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[Documents]":
        model_name = config.get("model_name")

        if model_name is None:
            assert False, "This code should not be reached"

        return PylateColBERTEmbeddingFunction(model_name=model_name)

    def get_config(self) -> Dict[str, Any]:
        return {"model_name": self.model_name}

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        if "model_name" in new_config:
            raise ValueError(
                "The model name cannot be changed after the embedding function has been initialized."
            )

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the configuration using the JSON schema.

        Args:
            config: Configuration to validate

        Raises:
            ValidationError: If the configuration does not match the schema
        """
        validate_config_schema(config, "pylate_colbert")
