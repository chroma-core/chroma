from abc import abstractmethod
from typing import Sequence
from chromadb.config import Component
from chromadb.execution.expression.plan import CountPlan, GetPlan, KNNPlan
from chromadb.types import MetadataEmbeddingRecord, VectorEmbeddingRecord


class Executor(Component):
    @abstractmethod
    def count(self, plan: CountPlan) -> int:
        pass

    @abstractmethod
    def get(self, plan: GetPlan) -> Sequence[MetadataEmbeddingRecord]:
        pass

    @abstractmethod
    def knn(self, plan: KNNPlan) -> Sequence[Sequence[VectorEmbeddingRecord]]:
        pass
