from chromadb.api.types import SparseEmbeddings


def _sort_sparse_vectors(vectors: SparseEmbeddings) -> None:
    """Sort sparse vectors by indices in-place.

    Note: Since SparseVector is now a dataclass, we need to modify the lists directly.
    The dataclass fields are mutable lists, so this works in-place.
    """
    for vector in vectors:
        items = sorted(zip(vector.indices, vector.values), key=lambda pair: pair[0])
        if items:
            indices, values = zip(*items)
            vector.indices[:] = list(indices)
            vector.values[:] = list(values)
