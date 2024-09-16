import logging
from typing import Optional, cast

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings

logger = logging.getLogger(__name__)


class InstructorEmbeddingFunction(EmbeddingFunction[Documents]):
    # If you have a GPU with at least 6GB try model_name = "hkunlp/instructor-xl" and device = "cuda"
    # for a full list of options: https://github.com/HKUNLP/instructor-embedding#model-list
    def __init__(
        self,
        model_name: str = "hkunlp/instructor-base",
        device: str = "cpu",
        instruction: Optional[str] = None,
    ):
        try:
            from InstructorEmbedding import INSTRUCTOR
        except ImportError:
            raise ValueError(
                "The InstructorEmbedding python package is not installed. Please install it with `pip install InstructorEmbedding`"
            )
        self._model = INSTRUCTOR(model_name, device=device)
        self._instruction = instruction

    def __call__(self, input: Documents) -> Embeddings:
        if self._instruction is None:
            return cast(Embeddings, [embedding for embedding in self._model.encode(input, convert_to_numpy=True)])

        texts_with_instructions = [[self._instruction, text] for text in input]

        return cast(Embeddings, [embedding for embedding in self._model.encode(texts_with_instructions, convert_to_numpy=True)])
