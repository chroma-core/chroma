from chromadb.segment import SegmentManager, VectorReader, MetadataReader
from chromadb.config import System
from chromadb.db.system import SysDB
from overrides import override


class LocalSegmentManager(SegmentManager):
    _sysdb: SysDB

    def __init__(self, system: System):
        self._sysdb = self.require(SysDB)
        super().__init__(system)

    @override
    def start(self) -> None:
        super().start()

    @override
    def stop(self) -> None:
        super().stop()
