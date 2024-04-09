import pytest

from chromadb.utils.embedding_functions import FastEmbedEmbeddingFunction

# Skip test if the 'fastembed' package is not installed is not installed
fastembed = pytest.importorskip("fastembed", reason="fastembed not installed")


def test_fastembed() -> None:
    ef = FastEmbedEmbeddingFunction(model_name="BAAI/bge-small-en-v1.5")
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert len(embeddings) == 2
    assert len(embeddings[0]) == 384
