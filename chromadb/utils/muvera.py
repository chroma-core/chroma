from chromadb.api.types import Embedding, Embeddings
from typing import List
import numpy as np


class MT19937Compatible:

    """A simple MT19937-compatible random generator for consistency with C++"""

    def __init__(self, seed):
        self.rng = np.random.MT19937(seed)
        self.gen = np.random.Generator(self.rng)

    def randint(self, low, high):
        """Generate random integer in [low, high)"""
        return self.gen.integers(low, high)

    def bernoulli(self):
        """Generate random boolean bernoulli distribution"""
        return self.gen.random() < 0.5

    def normal(self, size):
        """Generate normal distributed numbers"""
        return self.gen.normal(0.0, 1.0, size).astype(np.float32)


def create_fdes(
    documents_multivec_embeddings: List[Embeddings],
    dims: int,
    k_sim: int = 5,
    num_repetitions: int = 3,
    is_query: bool = False,
    proj_dim: int = 8,
    final_dim: int = 5120,
    seed: int = 42,
) -> Embeddings:
    """
    Create FDEs (fixed dimensional encoding) for a list of multivec embeddings.

    Args:
        documents_multivec_embeddings (List[Embeddings]): A list of multivec embeddings for len(documents_multivec_embeddings) documents.
        dims (int): The dimension of the individual embeddings.
        k_sim (int): The number of gaussian vectors to use for simhash.
        num_repetitions (int): The number of times to repeat the FDE computation. This reduces the variance of the FDEs.
        is_query (bool): Whether the embeddings are for a query or not.
        proj_dim (int): The dimension of the projected embeddings.
        final_dim (int): The dimension of the final FDEs.
        seed (int): The seed for the random number generator.

    Returns:
        Embeddings: A list of FDEs, representing 1 FDE per document.
    """
    if is_query:
        return create_query_fdes(
            documents_multivec_embeddings,
            dims,
            k_sim,
            num_repetitions,
            proj_dim,
            final_dim,
            seed,
        )
    else:
        return create_document_fdes(
            documents_multivec_embeddings,
            dims,
            k_sim,
            num_repetitions,
            proj_dim,
            final_dim,
            seed,
        )


def create_query_fdes(
    documents_multivec_embeddings: List[Embeddings],
    dims: int,
    k_sim: int = 5,
    num_repetitions: int = 3,
    proj_dim: int = 8,
    final_dim: int = 5120,
    seed: int = 42,
) -> Embeddings:
    """Query FDE encoding"""

    if k_sim >= 31 or k_sim < 2:
        raise ValueError(f"Unsupported number of simhash projections: {k_sim}")

    num_clusters = 2**k_sim
    final_fdes: Embeddings = []

    for document_multivec_embeddings in documents_multivec_embeddings:
        doc_fde = np.zeros(num_repetitions * num_clusters * proj_dim, dtype=np.float32)

        for rep in range(num_repetitions):
            rng = MT19937Compatible(seed + rep)
            embeddings_matrix = np.array(document_multivec_embeddings, dtype=np.float32)

            # Generate matrices with correct RNG order
            simhash_matrix = generate_simhash_matrix(dims, k_sim, rng)
            sketch_results = embeddings_matrix @ simhash_matrix

            proj_matrix = generate_projection_matrix(dims, proj_dim, rng)
            projected_embeddings = embeddings_matrix @ proj_matrix

            # Process each point
            for point_idx in range(len(embeddings_matrix)):
                # Get partition index using Gray code
                partition_idx = compute_simhash_partition_index(
                    sketch_results[point_idx]
                )

                # Calculate index in the full output array
                index = rep * (num_clusters * proj_dim) + partition_idx * proj_dim

                # Add to partition (simple sum for queries)
                for k in range(proj_dim):
                    doc_fde[index + k] += projected_embeddings[point_idx, k]

        # Apply final count sketch if needed
        if final_dim < len(doc_fde):
            doc_fde = apply_count_sketch(doc_fde, final_dim, seed)

        final_fdes.append(doc_fde)

    return final_fdes


def create_document_fdes(
    documents_multivec_embeddings: List[Embeddings],
    dims: int,
    k_sim: int = 5,
    num_repetitions: int = 3,
    proj_dim: int = 8,
    final_dim: int = 5120,
    seed: int = 42,
    fill_empty_partitions: bool = False,
) -> Embeddings:
    """Document FDE encoding"""

    if k_sim >= 31 or k_sim < 2:
        raise ValueError(f"Unsupported number of simhash projections: {k_sim}")

    num_clusters = 2**k_sim
    final_fdes: Embeddings = []

    for document_multivec_embeddings in documents_multivec_embeddings:
        doc_fde = np.zeros(num_repetitions * num_clusters * proj_dim, dtype=np.float32)

        for rep in range(num_repetitions):
            rng = MT19937Compatible(seed + rep)
            embeddings_matrix = np.array(document_multivec_embeddings, dtype=np.float32)

            simhash_matrix = generate_simhash_matrix(dims, k_sim, rng)
            proj_matrix = generate_projection_matrix(dims, proj_dim, rng)

            sketch_results = embeddings_matrix @ simhash_matrix
            projected_embeddings = embeddings_matrix @ proj_matrix

            # Track partition sizes for this repetition
            partition_sizes = np.zeros(num_clusters)

            # First pass: accumulate sums and sizes
            for point_idx in range(len(embeddings_matrix)):
                partition_idx = compute_simhash_partition_index(
                    sketch_results[point_idx]
                )
                index = rep * (num_clusters * proj_dim) + partition_idx * proj_dim

                for k in range(proj_dim):
                    doc_fde[index + k] += projected_embeddings[point_idx, k]
                partition_sizes[partition_idx] += 1.0

            # Second pass: handle empty partitions and normalize
            for partition_idx in range(num_clusters):
                index = rep * (num_clusters * proj_dim) + partition_idx * proj_dim

                if partition_sizes[partition_idx] == 0.0 and k_sim > 0:
                    if fill_empty_partitions:
                        # Find closest point
                        closest_idx = find_closest_embedding_to_cluster(
                            partition_idx, sketch_results, projected_embeddings
                        )
                        for k in range(proj_dim):
                            doc_fde[index + k] = projected_embeddings[closest_idx, k]
                    # Note: empty partitions stay zero if not filling
                elif partition_sizes[partition_idx] > 0:
                    # Normalize to get centroid
                    for k in range(proj_dim):
                        doc_fde[index + k] /= partition_sizes[partition_idx]

        # Apply final count sketch if needed
        if final_dim < len(doc_fde):
            doc_fde = apply_count_sketch(doc_fde, final_dim, seed)

        final_fdes.append(doc_fde)

    return final_fdes


def generate_projection_matrix(
    input_dim: int, output_dim: int, rng: MT19937Compatible
) -> Embedding:
    """
    Generate AMS projection matrix.
    """

    proj_matrix = np.zeros((input_dim, output_dim), dtype=np.float32)

    for i in range(input_dim):
        # Pick random index in output dimension
        index = rng.randint(0, output_dim)
        # Pick random sign, matching bernoulli distribution
        sign = 2.0 * (1 if rng.bernoulli() else 0) - 1.0
        proj_matrix[i, index] = sign

    return proj_matrix


def generate_simhash_matrix(
    dims: int, k_sim: int, rng: MT19937Compatible
) -> np.ndarray:
    matrix = rng.normal((dims, k_sim))
    return matrix


def apply_count_sketch(vector: Embedding, final_dim: int, seed: int) -> Embedding:
    """Apply count sketch to vector."""

    rng = MT19937Compatible(seed)
    output = np.zeros(final_dim, dtype=np.float32)

    for i, val in enumerate(vector):
        index = rng.randint(0, final_dim)
        sign = 2.0 * (1 if rng.bernoulli() else 0) - 1.0
        output[index] += sign * val

    return output


def find_closest_embedding_to_cluster(
    cluster_id: int, sketch_results: np.ndarray, projected_embeddings: np.ndarray
) -> int:
    """
    Find the closest embedding to a cluster.
    """

    min_distance = float("inf")
    closest_embedding_idx = 0

    for i, sketch_vector in enumerate(sketch_results):
        distance = compute_distance_to_partition(sketch_vector, cluster_id)

        if distance < min_distance:
            min_distance = distance
            closest_embedding_idx = i

    return closest_embedding_idx


def compute_distance_to_partition(
    sketch_vector: np.ndarray, partition_index: int
) -> int:
    """Compute Hamming distance to a partition"""
    distance = 0
    binary_representation = gray_to_binary(partition_index)

    for i in range(len(sketch_vector) - 1, -1, -1):
        cur_bit = 1 if sketch_vector[i] > 0 else 0
        target_bit = binary_representation & 1
        if cur_bit != target_bit:
            distance += 1
        binary_representation >>= 1

    return distance


def compute_simhash_partition_index(sketch_vector: Embedding) -> int:
    """
    Compute the SimHash of an input embedding against a set of gaussian vectors.
    SimHash algorithm generates a binary vector of length len(g_vecs), creating a unique cluster ID.
    It assigns each embedding to one of the 2^len(g_vecs) clusters.

    Args:
        sketch_vector (Embedding): The sketch vector from simhash projection.

    Returns:
        int: The simhash value as an integer.
    """

    partition_index = 0
    for i in range(len(sketch_vector)):
        bit = 1 if sketch_vector[i] > 0 else 0
        partition_index = (partition_index << 1) + (bit ^ (partition_index & 1))
    return partition_index


def gray_to_binary(gray_code: int) -> int:
    """Convert Gray code to binary"""
    return gray_code ^ (gray_code >> 1)
