"""Hybrid search utilities combining BM25 keyword scoring with vector similarity.

Provides Reciprocal Rank Fusion (RRF) to merge keyword and semantic results
into a single ranked result set.
"""

import math
import re
from collections import Counter
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np


def tokenize(text: str) -> List[str]:
    """Tokenize text into lowercase alphanumeric terms.

    Args:
        text: The input text to tokenize.

    Returns:
        List of lowercase alphanumeric tokens.
    """
    if not text:
        return []
    return re.findall(r"[a-zA-Z0-9]+", text.lower())


class BM25Scorer:
    """BM25 keyword scorer for document ranking.

    BM25 (Best Match 25) is a bag-of-words retrieval function that ranks
    documents based on the query terms appearing in each document.

    Formulation:
        score(D, Q) = sum(IDF(q_i) * (tf(q_i, D) * (k1 + 1)) /
                        (tf(q_i, D) + k1 * (1 - b + b * |D| / avgdl)))
    where:
        - IDF(q_i) = log((N - n(q_i) + 0.5) / (n(q_i) + 0.5) + 1)
        - tf(q_i, D) = frequency of term q_i in document D
        - |D| = document length in tokens
        - avgdl = average document length
        - k1 = term frequency saturation parameter (default 1.5)
        - b = length normalization parameter (default 0.75)
    """

    def __init__(self, k1: float = 1.5, b: float = 0.75):
        self.k1 = k1
        self.b = b
        self._documents: List[str] = []
        self._tokenized: List[List[str]] = []
        self._doc_lengths: List[int] = []
        self._avgdl: float = 0.0
        self._idf: Dict[str, float] = {}
        self._doc_freqs: Counter = Counter()
        self._N: int = 0
        self._fitted: bool = False

    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text into lowercase terms."""
        return tokenize(text)

    def fit(self, documents: List[str]) -> None:
        """Fit the BM25 scorer on a document corpus.

        Args:
            documents: List of document strings to index.
        """
        self._documents = documents
        self._N = len(documents)

        if self._N == 0:
            self._fitted = True
            return

        # Tokenize all documents
        self._tokenized = [self._tokenize(doc) if doc else [] for doc in documents]
        self._doc_lengths = [len(tokens) for tokens in self._tokenized]
        self._avgdl = (
            sum(self._doc_lengths) / self._N if self._N > 0 else 0.0
        )

        # Compute document frequencies for IDF
        self._doc_freqs = Counter()
        for tokens in self._tokenized:
            unique_terms = set(tokens)
            for term in unique_terms:
                self._doc_freqs[term] += 1

        # Compute IDF for each term
        self._idf = {}
        for term, df in self._doc_freqs.items():
            # Smooth IDF: log((N - df + 0.5) / (df + 0.5) + 1)
            self._idf[term] = math.log(
                (self._N - df + 0.5) / (df + 0.5) + 1.0
            )

        self._fitted = True

    def score(self, query: str) -> List[Tuple[int, float]]:
        """Score all documents against a query string.

        Args:
            query: The query text.

        Returns:
            List of (document_index, score) tuples sorted by score descending.
            Only includes documents with score > 0.
        """
        if not self._fitted or self._N == 0:
            return []

        query_terms = self._tokenize(query)
        if not query_terms:
            return []

        results: List[Tuple[int, float]] = []

        for idx in range(self._N):
            doc_len = self._doc_lengths[idx]
            tokens = self._tokenized[idx]

            if doc_len == 0:
                continue

            # Count term frequencies in this document
            tf_in_doc = Counter(tokens)

            score = 0.0
            for term in query_terms:
                if term not in self._idf:
                    continue

                tf = tf_in_doc.get(term, 0)
                if tf == 0:
                    continue

                idf = self._idf[term]

                # BM25 term score
                numerator = tf * (self.k1 + 1.0)
                denominator = tf + self.k1 * (
                    1.0 - self.b + self.b * (doc_len / self._avgdl)
                )
                score += idf * numerator / denominator

            if score > 0:
                results.append((idx, score))

        # Sort by score descending
        results.sort(key=lambda x: x[1], reverse=True)
        return results

    def get_scores(self, query: str) -> List[float]:
        """Get BM25 scores for all documents.

        Args:
            query: The query text.

        Returns:
            List of scores for each document (0.0 for documents without matches).
        """
        scored = dict(self.score(query))
        return [scored.get(i, 0.0) for i in range(self._N)]


def reciprocal_rank_fusion(
    result_lists: List[List[Tuple[str, float]]],
    k: int = 60,
    weights: Optional[List[float]] = None,
) -> List[Tuple[str, float]]:
    """Merge multiple ranked result lists using Reciprocal Rank Fusion.

    RRF formula: score(d) = sum(weight_i / (k + rank_i(d)))
    where rank_i(d) is the rank (0-indexed) of document d in result list i.

    Args:
        result_lists: List of ranked result lists. Each inner list contains
                      (id, score) tuples sorted by best-first.
        k: RRF smoothing constant (default 60, standard in literature).
        weights: Optional per-list weights. If provided, must match number of
                result lists. Defaults to equal weighting.

    Returns:
        List of (id, fused_score) tuples sorted by score descending.
    """
    if not result_lists:
        return []

    n_lists = len(result_lists)
    if weights is None:
        weights = [1.0] * n_lists
    elif len(weights) != n_lists:
        raise ValueError(
            f"Number of weights ({len(weights)}) must match number of result lists ({n_lists})"
        )

    # Accumulate RRF scores per document
    fused: Dict[str, float] = {}

    for list_idx, result_list in enumerate(result_lists):
        weight = weights[list_idx]
        for rank, (doc_id, _original_score) in enumerate(result_list):
            rrf_score = weight / (k + rank + 1)  # rank is 0-indexed, convert to 1-indexed
            fused[doc_id] = fused.get(doc_id, 0.0) + rrf_score

    # Sort by fused score descending
    sorted_results = sorted(fused.items(), key=lambda x: x[1], reverse=True)
    return sorted_results


def normalize_scores(
    scored_results: List[Tuple[str, float]],
) -> List[Tuple[str, float]]:
    """Normalize scores to [0, 1] range.

    Args:
        scored_results: List of (id, score) tuples.

    Returns:
        List of (id, normalized_score) tuples.
    """
    if not scored_results:
        return []

    scores = [s for _, s in scored_results]
    min_score = min(scores)
    max_score = max(scores)

    if max_score == min_score:
        return [(doc_id, 1.0) for doc_id, _ in scored_results]

    return [
        (doc_id, (score - min_score) / (max_score - min_score))
        for doc_id, score in scored_results
    ]


def combine_ranked_lists(
    bm25_results: List[Tuple[str, float]],
    vector_results: List[Tuple[str, float]],
    alpha: float = 0.5,
) -> List[Tuple[str, float]]:
    """Combine BM25 and vector results using linear combination.

    This is an alternative to RRF that uses a weighted linear combination
    of normalized scores.

    Args:
        bm25_results: (id, score) tuples from BM25, sorted best-first.
        vector_results: (id, score) tuples from vector search, sorted best-first.
        alpha: Weight for vector scores (0-1). BM25 gets (1-alpha).

    Returns:
        List of (id, combined_score) tuples sorted by score descending.
    """
    alpha = max(0.0, min(1.0, alpha))

    # Normalize scores to [0, 1]
    bm25_norm = dict(normalize_scores(bm25_results))
    vector_norm = dict(normalize_scores(vector_results))

    # Combine all document IDs
    all_ids = set(bm25_norm.keys()) | set(vector_norm.keys())

    combined: List[Tuple[str, float]] = []
    for doc_id in all_ids:
        bm25_score = bm25_norm.get(doc_id, 0.0)
        vec_score = vector_norm.get(doc_id, 0.0)
        combined_score = (1.0 - alpha) * bm25_score + alpha * vec_score
        combined.append((doc_id, combined_score))

    combined.sort(key=lambda x: x[1], reverse=True)
    return combined
