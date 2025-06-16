"""
`query` is the function used by the frontend to search your documents
"""

import os

from chromadb.api.types import Document, EmbeddingFunction
from chromadb.utils.embedding_functions import (
    OpenAIEmbeddingFunction,
    JinaEmbeddingFunction,
)
import util

from modules.chunking import (
    CodeContext,
    chunk_code_using_tree_sitter,
    truncate_documents,
)
from modules.search import semantic_search_using_chroma
from modules.reranking import bm25, tf_idf, cross_encoder, rerank
from vars import REPO_NAME, COMMIT_HASH

import chromadb


def chunking(document: Document, context: CodeContext):
    return chunk_code_using_tree_sitter(document, context)
    # return chunk_code_with_expanded_context(document, context)
    # return dont_chunk(document, context)


def embedding_function() -> EmbeddingFunction:
    """
    Use any Chroma-compatible embedding function!
    https://docs.trychroma.com/docs/embeddings/embedding-functions
    """
    # return OpenAIEmbeddingFunction(model="text-embedding-3-small")
    return JinaEmbeddingFunction(
        api_key=os.getenv("JINA_API_KEY"),
        model_name="jina-embeddings-v3",
    )
    # return util.CodeBERTEmbeddingFunction()


client = chromadb.HttpClient()
collection_name = f"{REPO_NAME}_{COMMIT_HASH}".replace("/", "_")
collection = client.get_or_create_collection(
    name=collection_name, embedding_function=embedding_function()
)


def query(query: util.Query):
    results = semantic_search_using_chroma(query, collection)
    return results


if __name__ == "__main__":
    q = input("Enter your query: ")
    parsed_query = util.parse_query(q)
    results = query(parsed_query)
    print(results)
