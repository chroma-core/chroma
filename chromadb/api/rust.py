from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_TENANT, System
from overrides import override
import rust_bindings


class RustBindingsAPI(ServerAPI):
    def __init__(self, system: System):
        super().__init__(system)

    @override
    def heartbeat(self) -> int:
        # TODO: add pyi file for types
        return rust_bindings.heartbeat(1)  # type: ignore

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        rust_bindings.create_database(name, tenant)
