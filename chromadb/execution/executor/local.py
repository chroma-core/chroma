from chromadb.config import System
from chromadb.execution.executor.abstract import Executor
from chromadb.execution.expression.plan import CountPlan, GetPlan, KNNPlan
from chromadb.api.types import GetResult, QueryResult

ERROR_MSG = (
    "The Python local executor has been removed. Use Rust-based execution by"
    " running with chromadb.api.rust.RustBindingsAPI."
)


class LocalExecutor(Executor):
    def __init__(self, system: System):
        super().__init__(system)
        raise RuntimeError(ERROR_MSG)

    def count(self, plan: CountPlan) -> int:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)

    def get(self, plan: GetPlan) -> GetResult:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)

    def knn(self, plan: KNNPlan) -> QueryResult:  # type: ignore[override]
        raise RuntimeError(ERROR_MSG)
