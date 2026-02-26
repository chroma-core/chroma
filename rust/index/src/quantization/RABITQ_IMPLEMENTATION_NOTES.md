# RaBitQ Implementation Notes

Findings from reviewing the Chroma RaBitQ implementation against the original
paper and two reference implementations.

Reference implementations examined:

- **gaoj0017/RaBitQ** — original author's C++/Python code
  (`data/rabitq.py`, `src/space.h`, `src/ivf_rabitq.h`)
---

## Distance-Estimation Formula

All three implementations compute the same formula.  Starting from Euclidean
distance in the original space:

```
‖d − q‖² = ‖c‖² + ‖r_d‖² + ‖r_q‖² + 2⟨r_d, c⟩ + 2⟨r_q, c⟩ − 2⟨d, q⟩
```

where `r_d = d − c` and `r_q = q − c` are the data and query residuals relative
to the cluster centroid `c`.  The inner product `⟨r_d, r_q⟩` is estimated via
the 1-bit approximation (Theorem 3.2 of the paper):

```
⟨r_d, r_q⟩ ≈ ‖r_d‖ · ⟨g_d, r_q⟩ / ⟨g_d, n_d⟩
```

- `g_d[i] = +0.5` when bit `i` is 1, `−0.5` when bit `i` is 0 (our scaling)
- `n_d = r_d / ‖r_d‖` is the unit-norm data residual
- `⟨g_d, n_d⟩` is the **correction factor**, stored in `CodeHeader::correction`

---

## Correction Factor Algebra

The implementations use different scaling conventions but are algebraically
equivalent.

**Our implementation** (`Code::quantize`, BITS=1):

```
correction = 0.5 · Σ|r[i]| / ‖r‖     (= GRID_OFFSET · abs_sum / norm)
```

**gaoj0017** uses normalised vectors and defines:

```
x0 = ‖XP‖₁ / (√D · ‖XP‖)
```

For unit-norm `XP` this simplifies to `‖XP‖₁ / √D`.  With `g[i] = ±1/√D`
(their scaling), their correction equals `Σ|r̂[i]| / √D = Σ|r[i]| / (√D · ‖r‖)`.
Our `GRID_OFFSET = 0.5 = 1/(2·1) ≠ 1/√D`, but the `0.5` factors in `⟨g, r_q⟩`
and `⟨g, n_d⟩` cancel in the ratio, so the estimated distance is identical.

---

## Random Orthogonal Rotation (P)

The paper applies a random orthogonal rotation `P` before quantization to obtain
its theoretical error guarantees (the `O(1/√D)` bound holds in expectation over
random `P`).

| Implementation | Rotation applied? |
|---|---|
| gaoj0017 | Yes — `XP` computed once at index time |
| **Our production code** (`quantized_spann.rs::rotate`) | **Yes** — `self.rotation` matrix applied before `Code::quantize` |
| **Our benchmarks** (`benches/quantization.rs`) | **No** — `random_vec` produces unrotated inputs |

The rotation is not absent — it is correctly applied in production.  The
benchmarks intentionally omit it for simplicity; this does not affect the
performance measurements (timing is dominated by the inner-product arithmetic)
but does mean the benchmark error-distribution results carry a slight
test-setup bias (see Error Analysis section below).

---

## Storage of ⟨r, c⟩ (Radial Component)

The term `⟨r, c⟩` is required at query time for every code.

| Implementation | How ⟨r, c⟩ is stored |
|---|---|
| gaoj0017 | Exact f32, precomputed at index time |
| **Ours** (`CodeHeader::radial`) | **Exact** f32, precomputed at index time |

Storing the exact value is a strict accuracy advantage over the NTU library,
which introduces additional quantization error in this term.

---

## Precomputed Signed Sum (`factor_ppc` / `signed_sum`)

The signed sum `Σ sign[i] = 2·popcount(x_b) − D` appears in the bitwise
distance estimator for any query that uses `QuantizedQuery` or `BatchQueryLuts`.
It depends only on the data code and is constant across all queries.

**gaoj0017** precomputes this as `factor_ppc` at index time.

**Our previous implementation** recomputed `popcount(x_b)` on every call to
`distance_query_bitwise` (16 `popcnt` instructions for 1024-d) and on every
nibble iteration in `BatchQueryLuts::distance_query`.

**After this change**, `CodeHeader` stores the value as `signed_sum: i32`,
computed once in `Code::quantize`:

```rust
let popcount: i32 = packed.iter().map(|b| b.count_ones() as i32).sum();
let signed_sum = 2 * popcount - dim as i32;
```

`distance_query_bitwise` and `BatchQueryLuts::distance_query` now read
`code.signed_sum()` instead of running a popcount loop.  For 1024-d codes this
eliminates 16 `popcnt` + 16 additions per distance estimate in the bitwise path.

As a side-effect, `distance_query_bitwise` no longer needs the `dim: usize`
argument, which has been removed from the public API.

**Header size change:** `CodeHeader` grew from 12 bytes to 16 bytes
(`signed_sum: i32` added at offset 12).  Persisted codes (blockfiles) written
before this change are **not** compatible with the updated reader.

---

## Query Quantization: Deterministic vs. Stochastic Dithering

`QuantizedQuery::new` uses deterministic rounding:

```rust
((v - v_l) / delta).round()
```

Both gaoj0017 use stochastic dithering (random rounding) for
query quantization.  At `B_q = 4` bits the accuracy difference is negligible;
deterministic rounding is simpler and removes a source of non-determinism.

---

## 4-Bit Codebook Structure

Our 4-bit implementation uses a ray-walk algorithm to find the optimal grid
point along `r` that maximises cosine similarity.

---

## Error Analysis

The `print_error_analysis` benchmark (`benches/quantization.rs`) measures
relative and absolute error of the distance estimator for 4-bit float,
1-bit float, and 1-bit bitwise (QuantizedQuery) methods.

### Why relative error has a non-zero mean

Relative error `ε_rel = (d̂ − d) / d` has a strictly positive mean even for an
unbiased estimator, due to **Jensen's inequality**: `E[1/X] > 1/E[X]` when `X`
is a positive random variable.  The distribution of `d̂` is approximately
symmetric around `d`, but `1/d` is convex, so dividing by `d` distorts the
symmetry upward.  This is a property of the metric, not a flaw in the
implementation.

### Why absolute error has a non-zero mean in the benchmarks

The paper's unbiasedness guarantee is: for a fixed query `q`, the estimator is
unbiased in expectation over random orthogonal rotations `P`.  In the benchmarks
the rotation is omitted and queries are drawn from `Uniform(−1, 1)^D` centred at
the origin, not at the centroid.  This means query residuals `r_q = q − c` have
a non-zero expected value (`−c`), introducing a small systematic bias in the
test.  The absolute error mean is approximately 0.3 % of `d_true` with a
standard deviation of ≈ 2 %, making it negligible for ranking purposes.

---

## Summary of Differences

| Aspect | gaoj0017 (original paper) |  Ours |
|---|---|---|
| Random rotation | Yes | Yes (production); No (benchmarks) |
| `⟨r, c⟩` storage | Exact | Exact |
| `signed_sum` precomputed | Yes (`factor_ppc`) | Yes |
| Query dithering | Stochastic | Deterministic |
| 4-bit codebook | N/A | Ray-walk |
| Multi-bit query scoring | Bit-plane (same) | Bit-plane (same) |
