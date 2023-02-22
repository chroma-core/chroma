from typing import List, Optional, Tuple

from pydantic import PrivateAttr
from chromadb.api.models.Collection import Collection
from chromadb.api.types import QueryResult
from chromadb.experimental.density import IndexDensityDistribution

# This class provides a simple interface to experimental features of Chroma
class ExperimentalCollection(Collection):
    
    _base_collection: Collection = PrivateAttr()
    _index_density_distribution: Optional[IndexDensityDistribution] = PrivateAttr()

    def __init__(self, base_collection: Collection):
        self._base_collection = base_collection
        self._index_density_distribution = None

    def add(self, *args, **kwargs):
        self._index_density_distribution = None
        self._base_collection.add(*args, **kwargs)

    def update(self, *args, **kwargs):
        self._index_density_distribution = None
        self._base_collection.update(*args, **kwargs)

    def delete(self, *args, **kwargs):
        self_index_density_distribution = None
        self._base_collection.delete(*args, **kwargs)

    def query(self, *args, **kwargs) -> Tuple[QueryResult, Optional[List[float]]]:
        if self._index_density_distribution is None:
            self._index_density_distribution = IndexDensityDistribution(
                collection=self
            )
        result = self._base_collection.query(*args, **kwargs)

        density_percentiles = None
        if result['distances'] is not None:
            density_percentiles = self._index_density_distribution.evaluate_query(result['distances'])

        return result, density_percentiles
    
    # Delegate everything else to the base collection
    def __getattr__(self, name):
        return getattr(self._base_collection, name)
    