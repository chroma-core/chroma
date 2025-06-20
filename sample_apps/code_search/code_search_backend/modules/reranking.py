from util import Query
from modules.chunking import CodeChunk
import math
import re
from collections import Counter


def rerank(
    query: Query, documents: list[CodeChunk], ranking_function
) -> list[CodeChunk]:
    """Will rerank documents based on the given ranking function"""
    scores: list[float] = ranking_function(query, documents)
    docs_and_scores: list[tuple[CodeChunk, float]] = list(zip(documents, scores))
    docs_and_scores.sort(key=lambda entry: entry[1], reverse=True)
    return [doc for doc, _ in docs_and_scores]


def _tokenize(text: str) -> list[str]:
    """Simple tokenization: lowercase, split on non-alphanumeric characters"""
    text = text.lower()
    tokens = re.findall(r"\b\w+\b", text)
    return tokens


def _get_document_text(chunk: CodeChunk) -> str:
    """Get the text to use for ranking from a CodeChunk"""
    # Prefer index_document if available, otherwise use source_code
    return chunk.index_document or chunk.source_code


"""
The following functions return scores associated with each document that can be used to compare and rank them.
"""


def bm25(query: Query, documents: list[CodeChunk]) -> list[float]:
    """Implement BM25 algorithm for ranking documents"""
    if not documents:
        return []

    # BM25 parameters
    k1 = 1.2  # Controls term frequency scaling
    b = 0.75  # Controls document length normalization

    # Tokenize query
    query_terms = _tokenize(query.natural_language_query)
    if not query_terms:
        return [0.0] * len(documents)

    # Tokenize documents and calculate term frequencies
    doc_tokens = [_tokenize(_get_document_text(doc)) for doc in documents]
    doc_lengths = [len(tokens) for tokens in doc_tokens]
    avg_doc_length = sum(doc_lengths) / len(doc_lengths) if doc_lengths else 0

    # Calculate document frequencies for each term
    doc_freq = {}
    for tokens in doc_tokens:
        unique_terms = set(tokens)
        for term in unique_terms:
            doc_freq[term] = doc_freq.get(term, 0) + 1

    scores = []
    N = len(documents)  # Total number of documents

    for i, doc_tokens_list in enumerate(doc_tokens):
        doc_length = doc_lengths[i]
        term_freq = Counter(doc_tokens_list)

        score = 0.0
        for term in query_terms:
            if term in term_freq:
                tf = term_freq[term]
                df = doc_freq.get(term, 0)

                if df > 0:
                    # IDF calculation
                    idf = math.log((N - df + 0.5) / (df + 0.5))

                    # BM25 term score
                    numerator = tf * (k1 + 1)
                    denominator = tf + k1 * (1 - b + b * (doc_length / avg_doc_length))
                    term_score = idf * (numerator / denominator)
                    score += term_score

        scores.append(score)

    return scores


def tf_idf(query: Query, documents: list[CodeChunk]) -> list[float]:
    """Implement TF-IDF algorithm for ranking documents"""
    if not documents:
        return []

    # Tokenize query
    query_terms = _tokenize(query.natural_language_query)
    if not query_terms:
        return [0.0] * len(documents)

    # Tokenize documents
    doc_tokens = [_tokenize(_get_document_text(doc)) for doc in documents]

    # Calculate document frequencies for each term
    doc_freq = {}
    for tokens in doc_tokens:
        unique_terms = set(tokens)
        for term in unique_terms:
            doc_freq[term] = doc_freq.get(term, 0) + 1

    scores = []
    N = len(documents)  # Total number of documents

    for doc_tokens_list in doc_tokens:
        term_freq = Counter(doc_tokens_list)
        doc_length = len(doc_tokens_list)

        score = 0.0
        for term in query_terms:
            if term in term_freq:
                tf = term_freq[term]
                df = doc_freq.get(term, 0)

                if df > 0 and doc_length > 0:
                    # TF calculation (normalized by document length)
                    tf_normalized = tf / doc_length

                    # IDF calculation
                    idf = math.log(N / df)

                    # TF-IDF score
                    term_score = tf_normalized * idf
                    score += term_score

        scores.append(score)

    return scores
