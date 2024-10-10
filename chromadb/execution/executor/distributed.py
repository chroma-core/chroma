from typing import Sequence
from overrides import overrides
from chromadb.config import System
from chromadb.execution.executor.abstract import Executor
from chromadb.execution.expression.plan import CountPlan, GetPlan, KNNPlan
from chromadb.segment import SegmentManager
from chromadb.types import MetadataEmbeddingRecord, VectorEmbeddingRecord


class DistributedExecutor(Executor):
    _manager: SegmentManager

    def __init__(self, system: System):
        super().__init__(system)
        self._manager = self.require(SegmentManager)

    @overrides
    def count(self, plan: CountPlan) -> int:
        return 0

    @overrides
    def get(self, plan: GetPlan) -> Sequence[MetadataEmbeddingRecord]:
        return list()

    @overrides
    def knn(self, plan: KNNPlan) -> Sequence[Sequence[VectorEmbeddingRecord]]:
        return list()
