from chromadb.api.models.Collection import Collection
from chromadb.api.types import Documents
from util import Query
from main import embedding_function

from search import semantic_search_using_chroma
from reranking import bm25, tf_idf, cross_encoder, rerank

# Embedding Functions

codeBert = util.CodeBERTEmbeddingFunction()

# Search strategies


def semantic_search_only(query: Query, collection: Collection):
    results = semantic_search_using_chroma(query)
    return results


def semantic_search_with_bm25_reranking(query: Query, collection: Collection):
    results = semantic_search_using_chroma(query)
    results = rerank(results, bm25)
    return results


def semantic_search_with_tf_idf_reranking(query: Query, collection: Collection):
    results = semantic_search_using_chroma(query)
    results = rerank(results, tf_idf)
    return results


embedding_functions = [codeBert]
retrieval_models = [
    semantic_search_only,
    semantic_search_with_bm25_reranking,
    semantic_search_with_tf_idf_reranking,
]


def benchmark():
    ef = embedding_function()

    code_collection = client.get_or_create_collection(
        name=CHROMA_COLLECTION_NAME, embedding_function=ef
    )
    successful, total = 0, 0
    data = code_collection.get()
    assert data["documents"] != None and data["metadatas"] != None
    for id, document, metadata in zip(
        data["ids"], data["documents"], data["metadatas"]
    ):
        assert type(metadata["docstring"]) == str
        res = code_collection.query(query_texts=[metadata["docstring"]], n_results=100)
        if id in res["ids"][0]:
            successful += 1
        total += 1
        print(f"{successful / total:.3f}")


if __name__ == "__main__":
    benchmark()
