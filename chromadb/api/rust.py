from chromadb.api import ServerAPI
from chromadb.config import DEFAULT_TENANT, System
from overrides import override
from rust_bindings import Bindings


class RustBindingsAPI(ServerAPI):
    def __init__(self, system: System):
        super().__init__(system)
        self.bindings = Bindings()

    @override
    def heartbeat(self) -> int:
        # TODO: add pyi file for types
        return self.bindings.heartbeat()  # type: ignore

    @override
    def create_database(self, name: str, tenant: str = DEFAULT_TENANT) -> None:
        self.bindings.create_database(name, tenant)
