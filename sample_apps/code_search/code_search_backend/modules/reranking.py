from util import Query
from modules.chunking import CodeChunk

def rerank(query: Query, documents: list[CodeChunk], ranking_function) -> list[CodeChunk]:
    """Will rerank documents based on the given ranking function"""
    scores: list[float] = ranking_function(query, documents)
    docs_and_scores: list[tuple[CodeChunk, float]] = list(zip(documents, scores))
    docs_and_scores.sort(key=lambda entry: entry[1], reverse=True)
    return [doc for doc, _ in docs_and_scores]



"""
The following functions return scores associated with each document that can be used to compare and rank them.
"""

def bm25(query: Query, documents: list[CodeChunk]) -> list[float]:
    # Implement BM25 algorithm here
    pass

def tf_idf(query: Query, documents: list[CodeChunk]) -> list[float]:
    # Implement TF-IDF algorithm here
    pass

def cross_encoder(query: Query, documents: list[CodeChunk]) -> list[float]:
    # Implement Cross-Encoder algorithm here
    pass
