import util

from search import semantic_search_using_chroma
from reranking import bm25, tf_idf, cross_encoder, rerank


def query(query: Query):
    parsed_query = util.parse_query(query)
    results = semantic_search_using_chroma(parsed_query)
    return results

if __name__ == '__main__':
    q = input("Enter your query: ")
    parsed_query = util.parse_query(q)
    results = query(parsed_query)
    print(results)
