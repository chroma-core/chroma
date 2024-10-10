from dataclasses import dataclass

from chromadb.execution.expression.operator import Filter, KNN, Limit, Projection, Scan


@dataclass
class CountPlan:
    scan: Scan


@dataclass
class GetPlan:
    scan: Scan
    filter: Filter = Filter()
    limit: Limit = Limit()
    projection: Projection = Projection()


@dataclass
class KNNPlan:
    scan: Scan
    knn: KNN
    filter: Filter = Filter()
    projection: Projection = Projection()
