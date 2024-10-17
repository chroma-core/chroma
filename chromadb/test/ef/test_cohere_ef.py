import os

import pytest

from chromadb.utils.embedding_functions.cohere_embedding_function import (
    CohereEmbeddingFunction,
)

# Skip test if the 'fastembed' package is not installed is not installed
cohere = pytest.importorskip("cohere", reason="cohere not installed")


@pytest.mark.skipif(not os.getenv("COHERE_API_KEY"), reason="COHERE_API_KEY is not set")
def test_cohere_embedding_function():
    cohere_ef = CohereEmbeddingFunction(
        api_key=os.getenv("COHERE_API_KEY"), model_name="embed-multilingual-v3.0"
    )
    embeddings = cohere_ef(["This is a test doc"])
    assert len(embeddings) == 1
