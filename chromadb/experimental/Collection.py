from typing import List, Optional, Tuple
from chromadb.api.models.Collection import Collection
from chromadb.api.types import QueryResult
from chromadb.experimental.density import IndexDensityDistribution

# This class provides a simple interface to experimental features of Chroma
class ExperimentalCollection(Collection):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._index_density_distribution = None

    def add(self, *args, **kwargs):
        self._index_density_distribution = None
        super().add(*args, **kwargs)

    def update(self, *args, **kwargs):
        self._index_density_distribution = None
        super().update(*args, **kwargs)

    def query(self, *args, **kwargs) -> Tuple[QueryResult, Optional[List[float]]]:
        if self._index_density_distribution is None:
            self._index_density_distribution = IndexDensityDistribution(
                collection=self
            )
        result = super().query(*args, **kwargs)

        density_percentiles = None
        if result['distances'] is not None:
            density_percentiles = self._index_density_distribution.evaluate_query(result['distances'])

        return result, density_percentiles