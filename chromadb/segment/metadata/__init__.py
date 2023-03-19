from abc import ABC, abstractmethod
from typing import Sequence, Union, Optional, Literal
from chromadb.segment import InsertType, EmbeddingRecord


# Query Grammar
LiteralValue = Union[str, int, float]
LogicalOperator = Literal["$and", "$or"]
WhereOperator = Literal["$gt", "$gte", "$lt", "$lte", "$ne", "$eq"]
OperatorExpression = dict[Union[WhereOperator, LogicalOperator], LiteralValue]

Where = dict[Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, list["Where"]]]

WhereDocumentOperator = Literal["$contains", LogicalOperator]
WhereDocument = dict[WhereDocumentOperator, Union[str, list["WhereDocument"]]]


class MetadataWriter(ABC):
    """Write-side of an the Embedding Metadata segment interface"""

    @abstractmethod
    def insert_metadata(self,
                        metadata: Sequence[EmbeddingRecord],
                        insert_type: InsertType = InsertType.ADD_OR_UPDATE) -> None:
        """Insert embedding metadata"""
        pass


    @abstractmethod
    def delete_metadata(self, ids: Sequence[str]) -> None:
        """Soft-delete embedding metadata by ID"""
        pass


class MetadataReader(ABC):
    """Read-side of an Embedding Metadata segment interface"""

    @abstractmethod
    def get_metadata(self,
                     where: Where = {},
                     where_document: WhereDocument = {},
                     ids: Optional[Sequence[str]] = None,
                     sort: Optional[str] = None,
                     limit: Optional[int] = None,
                     offset: Optional[int] = None) -> Sequence[EmbeddingRecord]:
        """Query for embedding metadata."""
        pass

    @abstractmethod
    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        pass




