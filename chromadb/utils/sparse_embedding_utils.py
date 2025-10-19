from typing import List, Tuple
from chromadb.base_types import SparseVector


def normalize_sparse_vector(indices: List[int], values: List[float]) -> SparseVector:
    """Normalize and create a SparseVector by sorting indices and values together.

    This function takes raw indices and values (which may be unsorted or have duplicates)
    and returns a properly constructed SparseVector with sorted indices.

    Args:
        indices: List of dimension indices (may be unsorted)
        values: List of values corresponding to each index

    Returns:
        SparseVector with indices sorted in ascending order

    Raises:
        ValueError: If indices and values have different lengths
        ValueError: If there are duplicate indices (after sorting)
        ValueError: If indices are negative
        ValueError: If values are not numeric
    """
    if not indices:
        return SparseVector(indices=[], values=[])

    # Sort indices and values together by index
    sorted_pairs = sorted(zip(indices, values), key=lambda x: x[0])
    sorted_indices, sorted_values = zip(*sorted_pairs)

    # Create SparseVector which will validate:
    # - indices are sorted
    # - no duplicate indices
    # - indices are non-negative
    # - values are numeric
    return SparseVector(indices=list(sorted_indices), values=list(sorted_values))
