"""Unit tests for the Maximal Marginal Relevance (MMR) selection core."""

from typing import List

import numpy as np
import pytest
from numpy.typing import NDArray

from chromadb.utils.mmr import maximal_marginal_relevance

_EPS = 1e-30


def _reference_mmr(
    query: NDArray[np.float32],
    candidates: NDArray[np.float32],
    k: int,
    lambda_mult: float,
) -> List[int]:
    """A deliberately naive O(k * n * k) reference implementation.

    Used as an independent oracle to fuzz-check the optimized incremental
    implementation in ``chromadb.utils.mmr``. Tie-breaking favors the lower
    index, matching numpy's ``argmax``.
    """
    candidates = np.asarray(candidates, dtype=np.float32)
    n = len(candidates)
    if n == 0 or k <= 0:
        return []
    q = np.asarray(query, dtype=np.float32).reshape(-1)
    q = q / (np.linalg.norm(q) + _EPS)
    normed = candidates / (np.linalg.norm(candidates, axis=1, keepdims=True) + _EPS)
    relevance = normed @ q
    target = min(k, n)
    selected: List[int] = []
    remaining = list(range(n))
    while len(selected) < target:
        if not selected:
            best = max(remaining, key=lambda i: (relevance[i], -i))
        else:

            def score(i: int) -> float:
                max_sim = max(float(normed[i] @ normed[j]) for j in selected)
                return lambda_mult * float(relevance[i]) - (1.0 - lambda_mult) * max_sim

            best = max(remaining, key=lambda i: (score(i), -i))
        selected.append(best)
        remaining.remove(best)
    return selected


def test_invalid_lambda_raises() -> None:
    candidates = np.eye(3, dtype=np.float32)
    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    with pytest.raises(ValueError):
        maximal_marginal_relevance(query, candidates, k=2, lambda_mult=1.5)
    with pytest.raises(ValueError):
        maximal_marginal_relevance(query, candidates, k=2, lambda_mult=-0.1)


def test_empty_candidates_returns_empty() -> None:
    query = np.array([1.0, 0.0], dtype=np.float32)
    empty = np.zeros((0, 2), dtype=np.float32)
    assert maximal_marginal_relevance(query, empty, k=5) == []


def test_invalid_embedding_shape_raises_clear_error() -> None:
    query = np.array([1.0, 0.0], dtype=np.float32)

    with pytest.raises(ValueError, match="candidate_embeddings must be a 2D array"):
        maximal_marginal_relevance(query, np.array([1.0, 0.0], dtype=np.float32), k=1)

    with pytest.raises(ValueError, match="must have the same dimension"):
        maximal_marginal_relevance(
            query,
            np.array([[1.0, 0.0, 0.0]], dtype=np.float32),
            k=1,
        )


def test_non_positive_k_returns_empty() -> None:
    query = np.array([1.0, 0.0], dtype=np.float32)
    candidates = np.eye(2, dtype=np.float32)
    assert maximal_marginal_relevance(query, candidates, k=0) == []
    assert maximal_marginal_relevance(query, candidates, k=-3) == []


def test_first_pick_is_most_relevant() -> None:
    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    # Candidate 1 is the closest to the query direction.
    candidates = np.array(
        [[0.0, 1.0, 0.0], [1.0, 0.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float32
    )
    selected = maximal_marginal_relevance(query, candidates, k=1, lambda_mult=0.5)
    assert selected == [1]


def test_k_larger_than_n_returns_all() -> None:
    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    candidates = np.array(
        [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]], dtype=np.float32
    )
    selected = maximal_marginal_relevance(query, candidates, k=10, lambda_mult=0.5)
    assert sorted(selected) == [0, 1, 2]
    assert len(selected) == 3


def test_lambda_one_is_pure_relevance_order() -> None:
    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    # Cosine similarity to query: idx0=1.0, idx1≈0.95, idx2≈0.6, idx3=0.0
    candidates = np.array(
        [
            [1.0, 0.0, 0.0],
            [0.95, 0.31, 0.0],
            [0.6, 0.8, 0.0],
            [0.0, 1.0, 0.0],
        ],
        dtype=np.float32,
    )
    selected = maximal_marginal_relevance(query, candidates, k=4, lambda_mult=1.0)
    # Pure relevance => descending cosine similarity to the query.
    assert selected == [0, 1, 2, 3]


def test_diversity_prefers_dissimilar_second_pick() -> None:
    query = np.array([1.0, 0.0, 0.0], dtype=np.float32)
    # idx0 is most relevant. idx1 is a near-duplicate of idx0; idx2 is slightly
    # less relevant but orthogonal. With diversity weighting, the second pick
    # should be the orthogonal idx2, not the redundant idx1.
    candidates = np.array(
        [
            [1.0, 0.0, 0.0],  # most relevant
            [0.99, 0.01, 0.0],  # near-duplicate of idx0
            [0.7, 0.0, 0.7],  # less relevant but diverse
        ],
        dtype=np.float32,
    )
    relevance_only = maximal_marginal_relevance(query, candidates, k=2, lambda_mult=1.0)
    diversified = maximal_marginal_relevance(query, candidates, k=2, lambda_mult=0.2)
    assert relevance_only == [0, 1]
    assert diversified == [0, 2]


def test_selection_is_a_permutation_subset() -> None:
    rng = np.random.default_rng(0)
    candidates = rng.standard_normal((20, 8)).astype(np.float32)
    query = rng.standard_normal(8).astype(np.float32)
    selected = maximal_marginal_relevance(query, candidates, k=7, lambda_mult=0.5)
    assert len(selected) == 7
    assert len(set(selected)) == 7  # no duplicates
    assert all(0 <= i < 20 for i in selected)


def test_matches_brute_force_reference() -> None:
    """The optimized incremental selection must match a naive oracle exactly.

    Random Gaussian vectors make ties a measure-zero event, so the selections
    (including order) should be identical across a wide range of shapes and
    lambda values.
    """
    rng = np.random.default_rng(12345)
    for _ in range(500):
        n = int(rng.integers(1, 40))
        d = int(rng.integers(2, 64))
        k = int(rng.integers(1, 45))
        lambda_mult = float(rng.choice([0.0, 0.1, 0.25, 0.5, 0.7, 0.9, 1.0]))
        candidates = rng.standard_normal((n, d)).astype(np.float32)
        query = rng.standard_normal(d).astype(np.float32)
        assert maximal_marginal_relevance(
            query, candidates, k, lambda_mult
        ) == _reference_mmr(query, candidates, k, lambda_mult)


def test_output_invariants_fuzz() -> None:
    rng = np.random.default_rng(99)
    for _ in range(500):
        n = int(rng.integers(1, 30))
        d = int(rng.integers(2, 32))
        k = int(rng.integers(1, 35))
        candidates = rng.standard_normal((n, d)).astype(np.float32)
        query = rng.standard_normal(d).astype(np.float32)
        selected = maximal_marginal_relevance(query, candidates, k, float(rng.random()))
        assert len(selected) == min(k, n)
        assert len(set(selected)) == len(selected)
        assert all(0 <= i < n for i in selected)


def test_permutation_invariance() -> None:
    """Shuffling candidates must not change the selected set (by content)."""
    rng = np.random.default_rng(5)
    for _ in range(200):
        n = int(rng.integers(2, 20))
        d = int(rng.integers(2, 16))
        k = int(rng.integers(1, n + 1))
        candidates = rng.standard_normal((n, d)).astype(np.float32)
        query = rng.standard_normal(d).astype(np.float32)
        perm = rng.permutation(n)
        base = maximal_marginal_relevance(query, candidates, k, 0.5)
        shuffled = maximal_marginal_relevance(query, candidates[perm], k, 0.5)
        base_set = {tuple(np.round(candidates[i], 5)) for i in base}
        shuffled_set = {tuple(np.round(candidates[perm][i], 5)) for i in shuffled}
        assert base_set == shuffled_set


def test_diversity_reduces_intra_set_similarity() -> None:
    """Lower lambda should not increase the average similarity among results."""

    def intra_similarity(embeddings: NDArray[np.float32], idx: List[int]) -> float:
        if len(idx) < 2:
            return 0.0
        sub = embeddings[idx]
        normed = sub / (np.linalg.norm(sub, axis=1, keepdims=True) + _EPS)
        sims = normed @ normed.T
        upper = np.triu_indices(len(idx), 1)
        return float(sims[upper].mean())

    rng = np.random.default_rng(2024)
    for _ in range(100):
        centers = rng.standard_normal((4, 24)).astype(np.float32)
        candidates = np.vstack(
            [centers[i % 4] + 0.05 * rng.standard_normal(24) for i in range(40)]
        ).astype(np.float32)
        query = rng.standard_normal(24).astype(np.float32)
        diverse = maximal_marginal_relevance(query, candidates, 8, 0.1)
        relevant = maximal_marginal_relevance(query, candidates, 8, 1.0)
        assert (
            intra_similarity(candidates, diverse)
            <= intra_similarity(candidates, relevant) + 1e-6
        )


def test_degenerate_inputs_do_not_crash() -> None:
    # All-identical candidates: still returns k distinct indices.
    identical = np.ones((6, 8), dtype=np.float32)
    selected = maximal_marginal_relevance(np.ones(8, dtype=np.float32), identical, 3)
    assert len(selected) == 3 and len(set(selected)) == 3

    # Zero vectors must not produce NaNs or raise.
    zeros = np.zeros((5, 4), dtype=np.float32)
    selected = maximal_marginal_relevance(np.zeros(4, dtype=np.float32), zeros, 3)
    assert len(selected) == 3

    # High dimensionality and k far exceeding n.
    rng = np.random.default_rng(1)
    candidates = rng.standard_normal((50, 1536)).astype(np.float32)
    query = rng.standard_normal(1536).astype(np.float32)
    assert len(maximal_marginal_relevance(query, candidates, 10)) == 10
    assert len(maximal_marginal_relevance(query, candidates, 1000)) == 50
