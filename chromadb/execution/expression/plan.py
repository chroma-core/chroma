from dataclasses import dataclass, field
from typing import List, Dict, Any

from chromadb.execution.expression.operator import (
    KNN, Filter, Limit, Projection, Scan, Score, Project, Val
)


@dataclass
class CountPlan:
    scan: Scan


@dataclass
class GetPlan:
    scan: Scan
    filter: Filter = field(default_factory=Filter)
    limit: Limit = field(default_factory=Limit)
    projection: Projection = field(default_factory=Projection)


@dataclass
class KNNPlan:
    scan: Scan
    knn: KNN
    filter: Filter = field(default_factory=Filter)
    projection: Projection = field(default_factory=Projection)


@dataclass
class SearchPayload:
    """Payload for hybrid search operations"""
    filter: Filter = field(default_factory=Filter)
    score: Score = field(default_factory=lambda: Val(value=0.0))
    limit: Limit = field(default_factory=Limit)
    project: Project = field(default_factory=Project)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the SearchPayload to a dictionary for JSON serialization"""
        return {
            "filter": self.filter.to_dict(),
            "score": self.score.to_dict(),
            "limit": self.limit.to_dict(),
            "project": self.project.to_dict()
        }
