import chromadb.utils.embedding_functions as ef

default_ef = ef.DefaultEmbeddingFunction()

print(default_ef(["test"]))
