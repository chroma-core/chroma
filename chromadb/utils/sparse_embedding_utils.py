from typing import Dict, List, Optional, Sequence, Tuple
from chromadb.base_types import SparseVector


def max_pool_sparse_vectors(vectors: Sequence[SparseVector]) -> SparseVector:
    """Combine multiple sparse vectors using element-wise max pooling.

    For each unique index across all input vectors, takes the maximum value.
    This is the standard way to combine SPLADE embeddings across chunks,
    consistent with how SPLADE uses max pooling internally across token
    positions within a single forward pass.

    Args:
        vectors: Sequence of SparseVector instances to combine.

    Returns:
        A single SparseVector with max-pooled values.

    Raises:
        ValueError: If vectors is empty.
    """
    if not vectors:
        raise ValueError("Cannot max pool an empty list of vectors")

    if len(vectors) == 1:
        return vectors[0]

    has_labels = vectors[0].labels is not None

    max_values: Dict[int, float] = {}
    max_labels: Dict[int, str] = {}

    for vec in vectors:
        for i, (idx, val) in enumerate(zip(vec.indices, vec.values)):
            if idx not in max_values or val > max_values[idx]:
                max_values[idx] = val
                if has_labels and vec.labels is not None:
                    max_labels[idx] = vec.labels[i]

    sorted_indices = sorted(max_values.keys())
    values = [max_values[i] for i in sorted_indices]
    labels = [max_labels[i] for i in sorted_indices] if has_labels else None

    return SparseVector(indices=sorted_indices, values=values, labels=labels)


def normalize_sparse_vector(
    indices: List[int], 
    values: List[float],
    labels: Optional[List[str]] = None
) -> SparseVector:
    """Normalize and create a SparseVector by sorting indices and values together.

    This function takes raw indices and values (which may be unsorted or have duplicates)
    and returns a properly constructed SparseVector with sorted indices.

    Args:
        indices: List of dimension indices (may be unsorted)
        values: List of values corresponding to each index
        labels: Optional list of string labels corresponding to each index

    Returns:
        SparseVector with indices sorted in ascending order

    Raises:
        ValueError: If indices and values have different lengths
        ValueError: If there are duplicate indices (after sorting)
        ValueError: If indices are negative
        ValueError: If values are not numeric
        ValueError: If labels is provided and has different length than indices
    """
    if not indices:
        return SparseVector(indices=[], values=[], labels=None)

    # Sort indices, values, and labels together by index
    if labels is not None:
        sorted_triples = sorted(zip(indices, values, labels), key=lambda x: x[0])
        sorted_indices, sorted_values, sorted_labels = zip(*sorted_triples)
        return SparseVector(
            indices=list(sorted_indices), 
            values=list(sorted_values),
            labels=list(sorted_labels)
        )
    else:
        sorted_pairs = sorted(zip(indices, values), key=lambda x: x[0])
        sorted_indices, sorted_values = zip(*sorted_pairs)
        return SparseVector(
            indices=list(sorted_indices), 
            values=list(sorted_values),
            labels=None
        )
