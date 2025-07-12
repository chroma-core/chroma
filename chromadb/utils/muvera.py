from chromadb.api.types import Embedding, Embeddings
from typing import List, Dict, Any


def create_fdes(
    multivec_embeddings: List[Embeddings], g_vecs: Embeddings, is_query: bool = False
) -> Embeddings:
    """
    Create FDEs (fixed dimensional encoding) for a list of multivec embeddings.

    Args:
        multivec_embeddings (List[Embeddings]): A list of multivec embeddings for len(multivec_embeddings) documents.
        g_vecs (Embeddings): A list of gaussian vectors.
        is_query (bool): Whether the embeddings are for a query or not.

    Returns:
        Embeddings: A list of FDEs, representing 1 FDE per document.
    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("numpy is required for FDE computation")

    num_clusters = 2 ** len(g_vecs)
    dims = len(multivec_embeddings[0][0])

    fdes: Embeddings = []

    for multivec_embedding in multivec_embeddings:
        # group embeddings by cluster
        clusters: Dict[int, Any] = {}
        for embedding in multivec_embedding:
            cluster_id = simhash(embedding, g_vecs)
            if cluster_id not in clusters:
                clusters[cluster_id] = []
            clusters[cluster_id].append(embedding)

        # a block is a "piece" of the fde, which later gets concatenated to form the final fde per document
        fde_blocks = []
        for cluster_id in range(num_clusters):
            if cluster_id in clusters:
                if is_query:
                    # if query, sum embeddings in cluster
                    block = np.sum(clusters[cluster_id], axis=0)
                else:
                    # if insert, take centroid of embeddings in cluster
                    block = np.mean(clusters[cluster_id], axis=0)
            else:
                # no embeddings in cluster
                if is_query:
                    block = np.zeros(dims)
                else:
                    block = find_closest_embedding_to_cluster(
                        cluster_id, multivec_embedding, g_vecs
                    )
            fde_blocks.append(block)

        fde: Embedding = np.concatenate(fde_blocks)
        fdes.append(fde)

    return fdes


def find_closest_embedding_to_cluster(
    cluster_id: int, multivec_embedding: Embeddings, g_vecs: Embeddings
) -> Embedding:
    """
    Find the closest embedding to a cluster.
    """
    min_distance = float("inf")
    closest_embedding = multivec_embedding[0]

    for embedding in multivec_embedding:
        embedding_cluster = simhash(embedding, g_vecs)

        hamming_distance = bin(cluster_id ^ embedding_cluster).count("1")
        if hamming_distance < min_distance:
            min_distance = hamming_distance
            closest_embedding = embedding

    return closest_embedding


def simhash(input_embedding: Embedding, g_vecs: Embeddings) -> int:
    """
    Compute the SimHash of an input embedding against a set of gaussian vectors.
    SimHash algorithm generates a binary vector of length len(g_vecs), creating a unique cluster ID.
    It assigns each embedding to one of the 2^len(g_vecs) clusters.

    Args:
        input_embedding (list): The input embedding vector.
        g_vecs (list): A list of random Gaussian vectors

    Returns:
        int: The simhash value as an integer.
    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("numpy is required for simhash computation")

    bits = []
    for g_vec in g_vecs:
        # Compute the dot product and determine the sign
        dot_product = np.dot(input_embedding, g_vec)
        bits.append(1 if dot_product > 0 else 0)

    # Convert the list of bits to decimal
    simhash_value = 0
    for i, bit in enumerate(bits):
        if bit:
            simhash_value += 1 << i

    return simhash_value


def generate_gaussian_vectors(dims: int, k_sim: int) -> Embeddings:
    """
    Generate a set of random Gaussian vectors.
    Gaussian vectors are used as hyperplanes to divide the embedding space into 2^k_sim clusters.
    """
    try:
        import numpy as np
    except ImportError:
        raise ImportError("numpy is required for simhash computation")
    return [np.random.randn(dims).astype(np.float32) for _ in range(k_sim)]
