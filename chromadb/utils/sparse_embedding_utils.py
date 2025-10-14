from chromadb.api.types import SparseEmbeddings


def _sort_sparse_vectors(vectors: SparseEmbeddings) -> None:
    for vector in vectors:
        items = sorted(
            zip(vector["indices"], vector["values"]), key=lambda pair: pair[0]
        )
        if items:
            indices, values = zip(*items)
            vector["indices"] = list(indices)
            vector["values"] = list(values)
