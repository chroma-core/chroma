from typing import List, Optional, Tuple
from chromadb.base_types import SparseVector


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
