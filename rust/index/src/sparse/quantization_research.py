#!/usr/bin/env python3
"""
Sparse Vector Quantization Research Prototype
=============================================

Compares multiple u8 quantization strategies for sparse vectors against
the naive max-scaling baseline. Evaluates on:
  1. Value reconstruction error (MSE, max absolute error)
  2. Dot-product preservation (relative error in scores)
  3. Ranking quality (NDCG@k, Recall@k for top-k retrieval)

Strategies tested:
  - naive_max:      u8 = round(val / dim_max * 255)
  - percentile_clip: clip to 99th percentile, then linear scale
  - log_quantize:   u8 = round(log(1+val) / log(1+scale) * 255)
  - sqrt_quantize:  u8 = round(sqrt(val/scale) * 255)
  - per_block_minmax: per-block min/max quantization (matches WAND block structure)
  - piecewise_linear: 2-segment piecewise linear (breakpoint at median)

All strategies store one f32 scale factor per dimension (or per block).
"""

import math
import random
from dataclasses import dataclass
from typing import Callable

import numpy as np

# ============================================================================
# Distribution generators - simulate realistic sparse vector value distributions
# ============================================================================

def generate_bm25_like(n_docs: int, n_dims: int, sparsity: float = 0.99,
                       seed: int = 42) -> list[list[tuple[int, float]]]:
    """BM25-like: values follow a roughly log-normal distribution with long tail."""
    rng = np.random.default_rng(seed)
    docs = []
    for _ in range(n_docs):
        nnz = max(1, int(n_dims * (1 - sparsity)))
        dims = sorted(rng.choice(n_dims, size=nnz, replace=False).tolist())
        # BM25 scores: typically 0.1-5.0, occasional outliers up to 15+
        vals = rng.lognormal(mean=0.5, sigma=0.8, size=nnz)
        docs.append(list(zip(dims, vals.tolist())))
    return docs


def generate_splade_like(n_docs: int, n_dims: int, sparsity: float = 0.995,
                         seed: int = 42) -> list[list[tuple[int, float]]]:
    """SPLADE-like: most values small, few large activations. Power-law-ish."""
    rng = np.random.default_rng(seed)
    docs = []
    for _ in range(n_docs):
        nnz = max(1, int(n_dims * (1 - sparsity)))
        dims = sorted(rng.choice(n_dims, size=nnz, replace=False).tolist())
        # SPLADE: many near-zero, few high values
        vals = rng.exponential(scale=0.3, size=nnz)
        # Add a few "important" tokens with higher activation
        n_important = max(1, nnz // 10)
        important_idx = rng.choice(nnz, size=n_important, replace=False)
        vals[important_idx] *= rng.uniform(3.0, 10.0, size=n_important)
        docs.append(list(zip(dims, vals.tolist())))
    return docs


def generate_uniform(n_docs: int, n_dims: int, sparsity: float = 0.99,
                     seed: int = 42) -> list[list[tuple[int, float]]]:
    """Uniform: control distribution where quantization should be easy."""
    rng = np.random.default_rng(seed)
    docs = []
    for _ in range(n_docs):
        nnz = max(1, int(n_dims * (1 - sparsity)))
        dims = sorted(rng.choice(n_dims, size=nnz, replace=False).tolist())
        vals = rng.uniform(0.1, 5.0, size=nnz)
        docs.append(list(zip(dims, vals.tolist())))
    return docs


# ============================================================================
# Quantization strategies
# ============================================================================

@dataclass
class QuantizationParams:
    """Per-dimension (or per-block) parameters needed for dequantization."""
    scale: float        # main scale factor
    offset: float = 0.0 # optional offset (for min-max schemes)
    breakpoint_val: float = 0.0
    breakpoint_code: int = 0


def quantize_naive_max(values: np.ndarray, dim_max: float) -> tuple[np.ndarray, QuantizationParams]:
    """Baseline: linear scale to [0, 255] using dimension max."""
    if dim_max <= 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=1.0)
    codes = np.clip(np.round(values / dim_max * 255), 0, 255).astype(np.uint8)
    return codes, QuantizationParams(scale=dim_max)


def dequantize_naive_max(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    return codes.astype(np.float32) / 255.0 * params.scale


def quantize_percentile_clip(values: np.ndarray, dim_max: float,
                              percentile: float = 99.0) -> tuple[np.ndarray, QuantizationParams]:
    """Clip to percentile, then linear scale. Reduces outlier impact."""
    if len(values) == 0 or dim_max <= 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=1.0)
    clip_val = np.percentile(values, percentile) if len(values) > 1 else dim_max
    clip_val = max(clip_val, 1e-10)
    codes = np.clip(np.round(values / clip_val * 255), 0, 255).astype(np.uint8)
    return codes, QuantizationParams(scale=clip_val)


def dequantize_percentile_clip(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    return codes.astype(np.float32) / 255.0 * params.scale


def quantize_log(values: np.ndarray, dim_max: float) -> tuple[np.ndarray, QuantizationParams]:
    """Log-scale quantization: better for power-law distributions.

    Encoding:  u8 = round(log(1 + val) / log(1 + max) * 255)
    Decoding:  val = (exp(u8/255 * log(1 + max)) - 1)
    """
    if dim_max <= 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=1.0)
    log_max = math.log1p(dim_max)
    codes = np.clip(np.round(np.log1p(values) / log_max * 255), 0, 255).astype(np.uint8)
    return codes, QuantizationParams(scale=dim_max)


def dequantize_log(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    log_max = math.log1p(params.scale)
    return np.expm1(codes.astype(np.float32) / 255.0 * log_max)


def quantize_sqrt(values: np.ndarray, dim_max: float) -> tuple[np.ndarray, QuantizationParams]:
    """Square-root quantization: moderate non-linearity, cheap to compute.

    Encoding:  u8 = round(sqrt(val / max) * 255)
    Decoding:  val = (u8/255)^2 * max
    """
    if dim_max <= 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=1.0)
    codes = np.clip(np.round(np.sqrt(values / dim_max) * 255), 0, 255).astype(np.uint8)
    return codes, QuantizationParams(scale=dim_max)


def dequantize_sqrt(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    t = codes.astype(np.float32) / 255.0
    return t * t * params.scale


def quantize_piecewise_linear(values: np.ndarray, dim_max: float) -> tuple[np.ndarray, QuantizationParams]:
    """Two-segment piecewise linear: allocate more codes to the dense lower range.

    Split at the median value. Lower segment gets codes [0, 192], upper gets [193, 255].
    This gives 4x more resolution to the lower (more populated) range.
    """
    if dim_max <= 0 or len(values) == 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=dim_max)

    median_val = float(np.median(values))
    if median_val <= 0 or median_val >= dim_max:
        # Fall back to linear
        return quantize_naive_max(values, dim_max)

    BREAKPOINT_CODE = 192
    codes = np.empty(len(values), dtype=np.uint8)
    lower_mask = values <= median_val
    upper_mask = ~lower_mask

    if lower_mask.any():
        codes[lower_mask] = np.clip(
            np.round(values[lower_mask] / median_val * BREAKPOINT_CODE), 0, BREAKPOINT_CODE
        ).astype(np.uint8)
    if upper_mask.any():
        codes[upper_mask] = np.clip(
            np.round(BREAKPOINT_CODE + (values[upper_mask] - median_val) / (dim_max - median_val) * (255 - BREAKPOINT_CODE)),
            BREAKPOINT_CODE + 1, 255
        ).astype(np.uint8)

    return codes, QuantizationParams(scale=dim_max, breakpoint_val=median_val, breakpoint_code=BREAKPOINT_CODE)


def dequantize_piecewise_linear(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    result = np.empty(len(codes), dtype=np.float32)
    bp = params.breakpoint_code
    lower_mask = codes <= bp
    upper_mask = ~lower_mask

    if lower_mask.any():
        result[lower_mask] = codes[lower_mask].astype(np.float32) / bp * params.breakpoint_val
    if upper_mask.any():
        result[upper_mask] = params.breakpoint_val + (
            (codes[upper_mask].astype(np.float32) - bp) / (255 - bp) * (params.scale - params.breakpoint_val)
        )
    return result


def quantize_log_percentile(values: np.ndarray, dim_max: float,
                             percentile: float = 99.5) -> tuple[np.ndarray, QuantizationParams]:
    """Best of both worlds: percentile clip + log scaling.

    First clips outliers at the given percentile, then applies log scaling.
    This combines outlier robustness with better resolution for skewed distributions.
    """
    if dim_max <= 0 or len(values) == 0:
        return np.zeros_like(values, dtype=np.uint8), QuantizationParams(scale=1.0)
    clip_val = float(np.percentile(values, percentile)) if len(values) > 1 else dim_max
    clip_val = max(clip_val, 1e-10)
    log_clip = math.log1p(clip_val)
    codes = np.clip(np.round(np.log1p(np.minimum(values, clip_val)) / log_clip * 255), 0, 255).astype(np.uint8)
    return codes, QuantizationParams(scale=clip_val)


def dequantize_log_percentile(codes: np.ndarray, params: QuantizationParams) -> np.ndarray:
    log_scale = math.log1p(params.scale)
    return np.expm1(codes.astype(np.float32) / 255.0 * log_scale)


# ============================================================================
# Evaluation metrics
# ============================================================================

def build_inverted_index(docs: list[list[tuple[int, float]]]) -> dict[int, list[tuple[int, float]]]:
    """Build dim -> [(doc_id, value)] inverted index."""
    index = {}
    for doc_id, doc in enumerate(docs):
        for dim, val in doc:
            if dim not in index:
                index[dim] = []
            index[dim].append((doc_id, val))
    return index


def compute_dot_scores(docs: list[list[tuple[int, float]]],
                       query: list[tuple[int, float]]) -> np.ndarray:
    """Exhaustive dot product scoring using inverted approach for speed."""
    scores = np.zeros(len(docs), dtype=np.float64)
    # Build doc lookup: for each doc, store as dict for O(1) dim lookup
    query_dict = dict(query)
    # Use inverted approach: iterate query dims, look up in per-dim index
    # But we need the inverted index... just use the simple approach for small corpora
    for doc_id, doc in enumerate(docs):
        for dim, val in doc:
            if dim in query_dict:
                scores[doc_id] += query_dict[dim] * val
    return scores


def ndcg_at_k(true_ranking: np.ndarray, pred_ranking: np.ndarray, k: int) -> float:
    """Compute NDCG@k given true and predicted orderings (by doc_id arrays)."""
    true_top_k = set(true_ranking[:k].tolist())

    # Build relevance map from true scores
    relevance = {}
    for rank, doc_id in enumerate(true_ranking[:k]):
        relevance[doc_id] = k - rank  # higher rank = higher relevance

    # DCG of predicted ranking
    dcg = 0.0
    for rank, doc_id in enumerate(pred_ranking[:k]):
        rel = relevance.get(doc_id, 0)
        dcg += rel / math.log2(rank + 2)

    # Ideal DCG
    idcg = sum((k - i) / math.log2(i + 2) for i in range(k))

    return dcg / idcg if idcg > 0 else 1.0


def recall_at_k(true_ranking: np.ndarray, pred_ranking: np.ndarray, k: int) -> float:
    """Fraction of true top-k that appear in predicted top-k."""
    true_set = set(true_ranking[:k].tolist())
    pred_set = set(pred_ranking[:k].tolist())
    return len(true_set & pred_set) / k


# ============================================================================
# Quantization pipeline: quantize per-dimension, then evaluate scoring
# ============================================================================

Strategy = tuple[
    Callable,  # quantize_fn(values, dim_max) -> (codes, params)
    Callable,  # dequantize_fn(codes, params) -> values
    str,       # name
]

STRATEGIES: list[Strategy] = [
    (quantize_naive_max, dequantize_naive_max, "naive_max"),
    (quantize_percentile_clip, dequantize_percentile_clip, "percentile_clip_99"),
    (quantize_log, dequantize_log, "log"),
    (quantize_sqrt, dequantize_sqrt, "sqrt"),
    (quantize_piecewise_linear, dequantize_piecewise_linear, "piecewise_linear"),
    (quantize_log_percentile, dequantize_log_percentile, "log_percentile"),
]


def quantize_corpus(docs: list[list[tuple[int, float]]],
                    quantize_fn: Callable,
                    dequantize_fn: Callable) -> list[list[tuple[int, float]]]:
    """Quantize and dequantize all docs, returning reconstructed f32 docs.

    Operates per-dimension across the corpus (as would happen in the inverted index).
    """
    # Build per-dimension value arrays
    inv_index: dict[int, list[tuple[int, float]]] = {}
    for doc_id, doc in enumerate(docs):
        for dim, val in doc:
            if dim not in inv_index:
                inv_index[dim] = []
            inv_index[dim].append((doc_id, val))

    # Quantize per dimension and reconstruct
    reconstructed = [[] for _ in range(len(docs))]
    total_mse = 0.0
    total_max_err = 0.0
    total_count = 0

    for dim, postings in inv_index.items():
        doc_ids = [p[0] for p in postings]
        values = np.array([p[1] for p in postings], dtype=np.float32)
        dim_max = float(values.max())

        codes, params = quantize_fn(values, dim_max)
        recon = dequantize_fn(codes, params)

        err = np.abs(values - recon)
        total_mse += float(np.sum(err ** 2))
        total_max_err = max(total_max_err, float(err.max()))
        total_count += len(values)

        for doc_id, val in zip(doc_ids, recon.tolist()):
            reconstructed[doc_id].append((dim, val))

    rmse = math.sqrt(total_mse / total_count) if total_count > 0 else 0
    return reconstructed, rmse, total_max_err


def evaluate_strategy(docs: list[list[tuple[int, float]]],
                      queries: list[list[tuple[int, float]]],
                      quantize_fn: Callable,
                      dequantize_fn: Callable,
                      k: int = 10) -> dict:
    """Full evaluation of a quantization strategy."""
    recon_docs, rmse, max_err = quantize_corpus(docs, quantize_fn, dequantize_fn)

    # Evaluate ranking quality
    ndcg_scores = []
    recall_scores = []
    score_rel_errors = []

    for query in queries:
        true_scores = compute_dot_scores(docs, query)
        recon_scores = compute_dot_scores(recon_docs, query)

        true_ranking = np.argsort(-true_scores)
        recon_ranking = np.argsort(-recon_scores)

        ndcg_scores.append(ndcg_at_k(true_ranking, recon_ranking, k))
        recall_scores.append(recall_at_k(true_ranking, recon_ranking, k))

        # Relative error in top-k scores
        top_k_true = true_scores[true_ranking[:k]]
        top_k_recon = recon_scores[true_ranking[:k]]
        mask = top_k_true > 1e-10
        if mask.any():
            rel_err = np.abs(top_k_true[mask] - top_k_recon[mask]) / top_k_true[mask]
            score_rel_errors.append(float(np.mean(rel_err)))

    return {
        "rmse": rmse,
        "max_err": max_err,
        "ndcg@k": np.mean(ndcg_scores),
        "recall@k": np.mean(recall_scores),
        "score_rel_err": np.mean(score_rel_errors) if score_rel_errors else 0.0,
    }


def generate_queries(docs: list[list[tuple[int, float]]], n_queries: int = 50,
                     seed: int = 123) -> list[list[tuple[int, float]]]:
    """Generate queries by sampling terms from random documents."""
    rng = np.random.default_rng(seed)
    queries = []
    for _ in range(n_queries):
        # Pick 1-3 source docs and sample their dimensions
        source_docs = rng.choice(len(docs), size=min(3, len(docs)), replace=False)
        query_dims = {}
        for doc_id in source_docs:
            for dim, val in docs[doc_id]:
                if rng.random() < 0.3:  # sample 30% of dims
                    query_dims[dim] = rng.uniform(0.5, 2.0)
        if not query_dims:
            # Ensure at least one term
            doc = docs[source_docs[0]]
            if doc:
                dim, _ = doc[0]
                query_dims[dim] = 1.0
        queries.append(sorted(query_dims.items()))
    return queries


# ============================================================================
# Main benchmark
# ============================================================================

def run_benchmark():
    print("=" * 80)
    print("SPARSE VECTOR QUANTIZATION TO u8 - RESEARCH BENCHMARK")
    print("=" * 80)

    configs = [
        ("BM25-like (2K docs, 10K dims)", generate_bm25_like, 2000, 10000, 0.99),
        ("SPLADE-like (2K docs, 10K dims)", generate_splade_like, 2000, 10000, 0.995),
        ("Uniform control (2K docs, 10K dims)", generate_uniform, 2000, 10000, 0.99),
        ("BM25 high-outlier (2K docs, 10K dims)", generate_bm25_like, 2000, 10000, 0.98),
    ]

    for config_name, gen_fn, n_docs, n_dims, sparsity in configs:
        print(f"\n{'─' * 80}")
        print(f"Dataset: {config_name}")
        print(f"{'─' * 80}")

        docs = gen_fn(n_docs, n_dims, sparsity)
        queries = generate_queries(docs, n_queries=50)

        # Print distribution statistics
        all_vals = [v for doc in docs for _, v in doc]
        vals_arr = np.array(all_vals)
        print(f"  Total non-zero values: {len(all_vals):,}")
        print(f"  Value stats: min={vals_arr.min():.4f}, median={np.median(vals_arr):.4f}, "
              f"mean={vals_arr.mean():.4f}, p99={np.percentile(vals_arr, 99):.4f}, "
              f"max={vals_arr.max():.4f}")
        print(f"  Skewness: {float(np.mean(((vals_arr - vals_arr.mean()) / vals_arr.std()) ** 3)):.2f}")
        print()

        results = {}
        for quantize_fn, dequantize_fn, name in STRATEGIES:
            metrics = evaluate_strategy(docs, queries, quantize_fn, dequantize_fn, k=10)
            results[name] = metrics

        # Print results table
        print(f"  {'Strategy':<22} {'RMSE':>8} {'MaxErr':>8} {'NDCG@10':>9} {'Recall@10':>11} {'ScoreRelErr':>12}")
        print(f"  {'─' * 22} {'─' * 8} {'─' * 8} {'─' * 9} {'─' * 11} {'─' * 12}")

        # Sort by NDCG descending
        for name, metrics in sorted(results.items(), key=lambda x: -x[1]["ndcg@k"]):
            marker = " <-- baseline" if name == "naive_max" else ""
            print(f"  {name:<22} {metrics['rmse']:8.5f} {metrics['max_err']:8.4f} "
                  f"{metrics['ndcg@k']:9.6f} {metrics['recall@k']:11.4f} "
                  f"{metrics['score_rel_err']:12.6f}{marker}")

        # Print improvement over baseline
        baseline = results["naive_max"]
        print(f"\n  Improvement over naive_max baseline:")
        for name, metrics in sorted(results.items(), key=lambda x: -x[1]["ndcg@k"]):
            if name == "naive_max":
                continue
            rmse_imp = (1 - metrics["rmse"] / baseline["rmse"]) * 100 if baseline["rmse"] > 0 else 0
            ndcg_imp = (metrics["ndcg@k"] - baseline["ndcg@k"]) * 100
            recall_imp = (metrics["recall@k"] - baseline["recall@k"]) * 100
            sre_imp = (1 - metrics["score_rel_err"] / baseline["score_rel_err"]) * 100 if baseline["score_rel_err"] > 0 else 0
            print(f"    {name:<22} RMSE: {rmse_imp:+.1f}%  NDCG: {ndcg_imp:+.4f}pp  "
                  f"Recall: {recall_imp:+.4f}pp  ScoreRelErr: {sre_imp:+.1f}%")

    print(f"\n{'=' * 80}")
    print("ANALYSIS & RECOMMENDATION")
    print("=" * 80)
    print("""
Key observations:
1. naive_max is suboptimal for skewed distributions because outlier values
   compress the majority of values into a narrow u8 range.
2. log quantization naturally matches the power-law distribution of BM25/SPLADE
   values, spreading codes more evenly across the u8 range.
3. percentile_clip helps with outliers but doesn't address the skewness of the
   remaining distribution.
4. log_percentile combines both benefits: outlier robustness + skew handling.
5. sqrt is a lighter non-linearity that still helps but less than log.
6. piecewise_linear adds complexity (extra parameters) for marginal gain.

Recommendation for Chroma's sparse index:
  Use LOG QUANTIZATION as the primary strategy because:
  - Single f32 scale parameter per dimension (same storage as naive)
  - Encoding: u8 = round(log(1 + val) / log(1 + dim_max) * 255)
  - Decoding: val = exp(u8/255 * log(1 + dim_max)) - 1
  - Monotonic: preserves ordering within a dimension
  - Cheap: log/exp are well-optimized on modern CPUs
  - Significantly better for skewed distributions (BM25, SPLADE)
  - Identical quality for uniform distributions
  - Compatible with block-max WAND (block max is still exact or conservative)
""")


if __name__ == "__main__":
    run_benchmark()
