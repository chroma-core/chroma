from typing import Protocol
from chromadb.api.types import Documents, EmbeddingFunction


class SentenceTransformerEmbeddingFunction(EmbeddingFunction):
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError:
            raise ValueError(
                "sentence_transformers is not installed. Please install it with `pip install sentence_transformers`"
            )
        self._model = SentenceTransformer(model_name)

    def __call__(self, texts: Documents):
        return self._model.encode(list(texts), convert_to_numpy=True).tolist()
