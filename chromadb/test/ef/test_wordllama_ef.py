from chromadb.utils.embedding_functions.wordllama_embedding_function import WordLlamaEmbeddingFunction


def test_wordllama() -> None:
    ef = WordLlamaEmbeddingFunction()
    embeddings = ef(["Here is an article about llamas...", "this is another article"])
    assert len(embeddings) == 2
