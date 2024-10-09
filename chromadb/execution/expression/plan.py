from dataclasses import dataclass

from chromadb.execution.expression.operator import Filter, KNN, Limit, Project, Scan


@dataclass
class CountPlan:
    scan: Scan


@dataclass
class GetPlan:
    scan: Scan
    filter: Filter = Filter()
    limit: Limit = Limit()
    project: Project = Project()


@dataclass
class KnnPlan:
    scan: Scan
    knn: KNN
    filter: Filter = Filter()
    project: Project = Project()
