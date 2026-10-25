"""Maximal Marginal Relevance (MMR) selection.

MMR re-ranks a set of retrieval candidates to balance *relevance* to the query
against *diversity* among the selected results, using the criterion introduced
by Carbonell & Goldstein (1998):

    MMR = argmax_{d in candidates \\ selected} [
        lambda * sim(d, query) - (1 - lambda) * max_{s in selected} sim(d, s)
    ]

This is the standard tool RAG / agent pipelines use to avoid spending an LLM's
context window on several near-duplicate chunks: instead of the top-k by raw
similarity (which are often slight variants of the same passage), MMR keeps the
most relevant result and then prefers results that add *new* information.

`lambda_mult` controls the trade-off:
  * ``1.0`` -> pure relevance (equivalent to ordinary top-k ranking)
  * ``0.0`` -> maximal diversity, relevance only used to seed the first pick

Similarity is cosine similarity, which is the conventional choice for MMR and
is independent of the collection's distance space.
"""

from typing import List

import numpy as np
from numpy.typing import NDArray

# Prevent division by zero when normalizing zero (or near-zero) vectors.
_NORM_EPS = 1e-30


def _normalize_rows(matrix: NDArray[np.float32]) -> NDArray[np.float32]:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / (norms + _NORM_EPS)


def maximal_marginal_relevance(
    query_embedding: NDArray[np.float32],
    candidate_embeddings: NDArray[np.float32],
    k: int,
    lambda_mult: float = 0.5,
) -> List[int]:
    """Select up to ``k`` candidate indices via Maximal Marginal Relevance.

    Args:
        query_embedding: The query vector, shape ``(d,)``.
        candidate_embeddings: Candidate vectors, shape ``(n, d)`` — typically the
            over-fetched top-``fetch_k`` nearest neighbours of the query.
        k: Maximum number of candidates to select.
        lambda_mult: Relevance/diversity trade-off in ``[0, 1]``. ``1.0`` is pure
            relevance; ``0.0`` is maximal diversity. Defaults to ``0.5``.

    Returns:
        Indices into ``candidate_embeddings`` of the selected candidates, ordered
        by selection (most relevant first). The length is ``min(k, n)``.

    Raises:
        ValueError: If ``lambda_mult`` is outside ``[0, 1]``.
    """
    if not 0.0 <= lambda_mult <= 1.0:
        raise ValueError(f"lambda_mult must be in the range [0, 1], got {lambda_mult}")

    candidates = np.asarray(candidate_embeddings, dtype=np.float32)
    if k <= 0:
        return []
    if candidates.ndim != 2 and candidates.size == 0:
        return []
    if candidates.ndim != 2:
        raise ValueError(
            "candidate_embeddings must be a 2D array with shape (num_candidates, "
            f"dimension), got shape {candidates.shape}"
        )
    if candidates.shape[0] == 0:
        return []

    num_candidates = candidates.shape[0]
    target = min(k, num_candidates)

    query = np.asarray(query_embedding, dtype=np.float32).reshape(-1)
    if query.shape[0] != candidates.shape[1]:
        raise ValueError(
            "query_embedding and candidate_embeddings must have the same "
            f"dimension, got {query.shape[0]} and {candidates.shape[1]}"
        )
    query = query / (np.linalg.norm(query) + _NORM_EPS)

    normalized = _normalize_rows(candidates)

    # Relevance of every candidate to the query, and the full candidate-candidate
    # similarity matrix. fetch_k is small (tens to low hundreds), so the O(n^2)
    # matrix is cheap and lets each greedy step be a vectorized lookup.
    relevance = normalized @ query
    pairwise = normalized @ normalized.T

    # Seed with the single most relevant candidate.
    first = int(np.argmax(relevance))
    selected = [first]
    remaining = [i for i in range(num_candidates) if i != first]

    # `max_sim_to_selected[i]` tracks the largest similarity between candidate `i`
    # and anything already selected, updated incrementally as we grow `selected`.
    max_sim_to_selected = pairwise[:, first].copy()

    while len(selected) < target and remaining:
        remaining_arr = np.fromiter(remaining, dtype=np.intp)
        mmr_scores = (
            lambda_mult * relevance[remaining_arr]
            - (1.0 - lambda_mult) * max_sim_to_selected[remaining_arr]
        )
        best = int(remaining_arr[int(np.argmax(mmr_scores))])

        selected.append(best)
        remaining.remove(best)
        max_sim_to_selected = np.maximum(max_sim_to_selected, pairwise[:, best])

    return selected
