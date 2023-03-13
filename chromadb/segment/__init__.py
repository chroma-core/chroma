from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TypedDict, Union
import numpy.typing as npt

IDSeq = Sequence[str]
Vector = Sequence[Union[int, float]]


class Query(TypedDict):
    """A KNN/ANN query to the segment"""
    vector: Vector
    k: int = 10
    approximate: bool = True
    allowed_ids: Optional[IDSeq] = None


class Reader(ABC):
    """Read part of Segment Driver interface, used to retrieve
    vectors, and to perform KNN/ANN searches."""


    @abstractmethod
    def get(self, ids: Optional[IDSeq]) -> tuple[IDSeq, npt.NDArray]:
        """Get embeddings from the segment. If no IDs are provided,
        all embeddings are returned."""
        pass

    @abstractmethod
    def query(self, queries: Sequence[Query]) -> Sequence[IDSeq]:
        """Given a list of Query embeddings, return the top-k nearest
        neighbors for each query. If ids are provided, filter results
        to include only those IDs."""
        pass


class Writer(ABC):
    """Write part of the segment driver interface, used to store and index vectors."""

    @abstractmethod
    def add(self, ids: IDSeq, vectors: npt.NDArray) -> None:
        """Add new embeddings to the segment."""
        pass
