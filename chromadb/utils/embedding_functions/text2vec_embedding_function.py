from typing import cast
from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
import logging

logger = logging.getLogger(__name__)


class Text2VecEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, model_name: str = "shibing624/text2vec-base-chinese"):
        try:
            from text2vec import SentenceModel
        except ImportError:
            raise ValueError(
                "The text2vec python package is not installed. Please install it with `pip install text2vec`"
            )
        self._model = SentenceModel(model_name_or_path=model_name)

    def __call__(self, input: Documents) -> Embeddings:
        return cast(
            Embeddings, self._model.encode(list(input), convert_to_numpy=True).tolist()
        )  # noqa E501
