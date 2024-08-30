import ctypes
from typing import cast
import numpy as np

from chromadb.api.types import Documents, EmbeddingFunction, Embeddings
from chromadb.utils.cpp_utils import load_cpp_lib


EMBEDDINGS_LIB = "embeddings"
embeddings_lib = load_cpp_lib(EMBEDDINGS_LIB)

# Function signatures
embeddings_lib.EmbeddingModel_new.argtypes = [ctypes.c_char_p]
embeddings_lib.EmbeddingModel_new.restype = ctypes.c_void_p

embeddings_lib.EmbeddingModel_free.argtypes = [ctypes.c_void_p]
embeddings_lib.EmbeddingModel_free.restype = None

embeddings_lib.EmbeddingModel_get_emb_dim.argtypes = [ctypes.c_void_p]
embeddings_lib.EmbeddingModel_get_emb_dim.restype = ctypes.c_size_t

embeddings_lib.EmbeddingModel_embed.argtypes = [
    ctypes.c_void_p,
    ctypes.POINTER(ctypes.c_char_p),
    ctypes.c_size_t,
    ctypes.POINTER(ctypes.c_float),
]
embeddings_lib.EmbeddingModel_free.restype = None


class LlamacppEmbeddingModel:
    def __init__(self, model_path: str) -> None:
        self.model_path = model_path
        self.model = embeddings_lib.EmbeddingModel_new(model_path.encode("utf-8"))

        if not self.model:
            raise Exception("Failed to load model")

    def __del__(self) -> None:
        embeddings_lib.EmbeddingModel_free(self.model)

    def _get_emb_dim(self) -> int:
        return cast(int, embeddings_lib.EmbeddingModel_get_emb_dim(self.model))

    def embed(self, prompts: Documents) -> Embeddings:
        n_prompts = len(prompts)
        encoded_prompts = [prompt.encode("utf-8") for prompt in prompts]
        arr = (ctypes.c_char_p * n_prompts)(*encoded_prompts)
        emb_dim = self._get_emb_dim()

        # Allocate memory for embeddings with numpy
        embeddings = np.zeros((n_prompts, emb_dim), dtype=np.float32)
        embeddings_ptr = embeddings.ctypes.data_as(ctypes.POINTER(ctypes.c_float))

        embeddings_lib.EmbeddingModel_embed(self.model, arr, n_prompts, embeddings_ptr)

        return cast(Embeddings, embeddings.tolist())


class LlamacppEmbeddingFunction(EmbeddingFunction[Documents]):
    def __init__(self, model_path: str) -> None:
        self.model = LlamacppEmbeddingModel(model_path)

    def __call__(self, input: Documents) -> Embeddings:
        return self.model.embed(input)
