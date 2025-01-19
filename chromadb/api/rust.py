from chromadb.api.segment import SegmentAPI
from chromadb.config import System
from overrides import override
import rust_bindings


class RustBindingsAPI(SegmentAPI):
    def __init__(self, system: System):
        super().__init__(system)

    @override
    def heartbeat(self) -> int:
        # TODO: add pyi file for types
        return rust_bindings.heartbeat(1)  # type: ignore
