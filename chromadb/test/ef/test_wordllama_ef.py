import pytest

from chromadb.utils.embedding_functions import WordLlamaEmbeddingFunction

wordllama = pytest.importorskip("wordllama", reason="wordllama not installed")


def test_wordllama() -> None:
    ef = WordLlamaEmbeddingFunction()
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert len(embeddings) == 2
