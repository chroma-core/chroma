from typing import List, TYPE_CHECKING
from chromadb.logger import logger

try:
    import numpy as np
except ImportError:
    raise ImportError("You need to install numpy to use density estimation. pip install numpy")

# Prevent a circular dependency. 
if TYPE_CHECKING:
    from chromadb.experimental.ExperimentalCollection import ExperimentalCollection

class IndexDensityDistribution:
    def __init__(self, collection: 'ExperimentalCollection', estimator_neighborhood: int = 10, n_bins: int = 100):
        logger.info(f"Creating density estimator for collection {collection.name}. This may take some time...")
        collection_count = collection.count()
        if collection_count <= estimator_neighborhood:
            raise ValueError(
                f"The collection must contain at least {estimator_neighborhood} embeddings to estimate the index density distribution")
        
        embeddings = collection.get()["embeddings"]
        collection_uuid = collection._client._db.get_collection_uuid_from_name(collection.name)

        _, dists = collection._client._db._idx.get_nearest_neighbors(
            collection_uuid=collection_uuid,
            query=embeddings,
            k=estimator_neighborhood,
        )

        # Drop the first element as it is the query itself and will have dist 0.
        dists = dists[:, 1:]

        # Compute the mean distances from neighbors for each embedding
        mean_dists = np.mean(dists, axis=1)

        # Compute the cumulative density histogram for mean distances, with n_bins bins
        hist, bin_edges = np.histogram(mean_dists, bins=n_bins, density=True)
        self._bin_edges = bin_edges
        self._cdf = np.cumsum(hist * np.diff(bin_edges))
        self._estimator_neighborhood = estimator_neighborhood
    
    def evaluate_query(self, query_dists: List[List[float]]) -> List[float]:
        np_dists = np.array(query_dists)

        # Log a warning if the number of neighbors is less than the estimator neighborhood
        if np_dists.shape[1] < self._estimator_neighborhood:
            logger.warning(f"The number of neighbors ({np_dists.shape[1]}) is less than the estimator neighborhood ({self._estimator_neighborhood}). Density results may be inaccurate.")

        mean_dists = np.mean(np_dists, axis=1)

        # For each query distance, determine which bin it falls into
        # TODO: This could be linearly interpolated to get a more accurate (?) cdf.
        bin_idx = np.digitize(mean_dists, self._bin_edges) - 1

        # Convert bin indices to percentiles
        percentiles = 1 - self._cdf[bin_idx]

        return percentiles.tolist()
