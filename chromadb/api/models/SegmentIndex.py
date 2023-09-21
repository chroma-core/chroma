from typing import Set, Sequence

from pydantic import BaseModel, PrivateAttr

from chromadb.api.types import AllowedIndexColumns, IndexType


class SegmentIndex(BaseModel):
    _name: str = PrivateAttr()
    _columns: Set[AllowedIndexColumns] = PrivateAttr()
    _index_type: IndexType = IndexType.METADATA

    def __init__(self, name: str, columns: Set[AllowedIndexColumns], index_type: IndexType):
        super().__init__(_name=name, _columns=columns, _index_type=index_type)

    def update(self, columns: Set[AllowedIndexColumns]):
        self._columns = columns
        pass

    def drop(self):
        pass


class SegmentIndexManager:

    def add(self, indices: Sequence[SegmentIndex]):
        pass

    def get(self, name: str) -> SegmentIndex:
        pass

    def list(self) -> Sequence[SegmentIndex]:
        pass

    def drop_all(self):
        pass

    def drop(self, index_names: Sequence[str]):
        pass
