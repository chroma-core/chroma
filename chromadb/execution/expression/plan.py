from dataclasses import dataclass, field

from typing import Optional

from chromadb.execution.expression.operator import KNN, Filter, Limit, Projection, Scan


@dataclass
class CountPlan:
    scan: Scan


@dataclass
class GetPlan:
    scan: Scan
    filter: Filter = field(default_factory=Filter)
    limit: Limit = field(default_factory=Limit)
    projection: Projection = field(default_factory=Projection)
    max_distance: Optional[float] = None


@dataclass
class KNNPlan:
    scan: Scan
    knn: KNN
    filter: Filter = field(default_factory=Filter)
    projection: Projection = field(default_factory=Projection)
    max_distance: Optional[float] = None
