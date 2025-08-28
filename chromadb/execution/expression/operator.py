from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Set, Any, Union

from chromadb.api.types import Embeddings, IDs, Include, SparseVector
from chromadb.types import (
    Collection,
    RequestVersionContext,
    Segment,
)


@dataclass
class Scan:
    collection: Collection
    knn: Segment
    metadata: Segment
    record: Segment

    @property
    def version(self) -> RequestVersionContext:
        return RequestVersionContext(
            collection_version=self.collection.version,
            log_position=self.collection.log_position,
        )


# Where expression types for filtering
@dataclass
class Where:
    """Base class for Where expressions (algebraic data type)"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Where expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")


@dataclass
class And(Where):
    """Logical AND of multiple where conditions"""
    conditions: List[Where]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$and": [c.to_dict() for c in self.conditions]}


@dataclass
class Or(Where):
    """Logical OR of multiple where conditions"""
    conditions: List[Where]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$or": [c.to_dict() for c in self.conditions]}


@dataclass
class Eq(Where):
    """Equality comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$eq": self.value}}


@dataclass
class Ne(Where):
    """Not equal comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$ne": self.value}}


@dataclass
class Gt(Where):
    """Greater than comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$gt": self.value}}


@dataclass
class Gte(Where):
    """Greater than or equal comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$gte": self.value}}


@dataclass
class Lt(Where):
    """Less than comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$lt": self.value}}


@dataclass
class Lte(Where):
    """Less than or equal comparison"""
    key: str
    value: Any
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$lte": self.value}}


@dataclass
class In(Where):
    """In comparison - value is in a list"""
    key: str
    values: List[Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$in": self.values}}


@dataclass
class Nin(Where):
    """Not in comparison - value is not in a list"""
    key: str
    values: List[Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$nin": self.values}}


@dataclass
class Contains(Where):
    """Contains comparison for document content"""
    content: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$contains": self.content}


@dataclass
class NotContains(Where):
    """Not contains comparison for document content"""
    content: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$not_contains": self.content}


@dataclass
class Regex(Where):
    """Regular expression matching"""
    key: str
    pattern: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$regex": self.pattern}}


@dataclass
class NotRegex(Where):
    """Negative regular expression matching"""
    key: str
    pattern: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$not_regex": self.pattern}}


@dataclass
class Filter:
    user_ids: Optional[IDs] = None
    where: Optional[Any] = None  # Old Where type from chromadb.types
    where_document: Optional[Any] = None  # Old WhereDocument type
    

@dataclass
class SearchFilter:
    """Filter configuration for the search endpoint"""
    query_ids: Optional[IDs] = None
    where_clause: Optional[Where] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the SearchFilter to a dictionary for JSON serialization"""
        result = {}
        if self.query_ids is not None:
            result["query_ids"] = self.query_ids
        if self.where_clause is not None:
            result["where_clause"] = self.where_clause.to_dict() # type: ignore[assignment]
        return result


@dataclass
class KNN:
    embeddings: Embeddings
    fetch: int


@dataclass
class Limit:
    offset: int = 0
    limit: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Limit to a dictionary for JSON serialization"""
        result = {"offset": self.offset}
        if self.limit is not None:
            result["limit"] = self.limit
        return result


@dataclass
class Projection:
    document: bool = False
    embedding: bool = False
    metadata: bool = False
    rank: bool = False
    uri: bool = False

    @property
    def included(self) -> Include:
        includes = list()
        if self.document:
            includes.append("documents")
        if self.embedding:
            includes.append("embeddings")
        if self.metadata:
            includes.append("metadatas")
        if self.rank:
            includes.append("distances")
        if self.uri:
            includes.append("uris")
        return includes # type: ignore[return-value] 


# Rank expression types for hybrid search
@dataclass
class Rank:
    """Base class for Rank expressions (algebraic data type)"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Score expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")


@dataclass
class Abs(Rank):
    """Absolute value of a rank"""
    rank: Rank
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$abs": self.rank.to_dict()}


@dataclass
class Div(Rank):
    """Division of two ranks"""
    left: Rank
    right: Rank
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$div": {"left": self.left.to_dict(), "right": self.right.to_dict()}}


@dataclass
class Exp(Rank):
    """Exponentiation of a rank"""
    rank: Rank
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$exp": self.rank.to_dict()}


@dataclass
class Log(Rank):
    """Logarithm of a rank"""
    rank: Rank
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$log": self.rank.to_dict()}


@dataclass
class Max(Rank):
    """Maximum of multiple ranks"""
    ranks: List[Rank]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$max": [r.to_dict() for r in self.ranks]}


@dataclass
class Min(Rank):
    """Minimum of multiple ranks"""
    ranks: List[Rank]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$min": [r.to_dict() for r in self.ranks]}


@dataclass
class Mul(Rank):
    """Multiplication of multiple ranks"""
    ranks: List[Rank]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$mul": [r.to_dict() for r in self.ranks]}


@dataclass
class Knn(Rank):
    """KNN-based ranking"""
    embedding: Union[List[float], SparseVector]
    key: str = "$chroma_embedding"
    limit: int = 1024
    default: Optional[float] = None
    ordinal: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        # With untagged enum, embedding is serialized directly
        # (as a list for dense, or as a dict with indices/values for sparse)
        result = {
            "embedding": self.embedding,
            "key": self.key,
            "limit": self.limit
        }
        
        if self.default is not None:
            result["default"] = self.default # type: ignore[assignment]
        if self.ordinal:
            result["ordinal"] = self.ordinal
        
        return {"$knn": result}


@dataclass
class Sub(Rank):
    """Subtraction of two ranks"""
    left: Rank
    right: Rank
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$sub": {"left": self.left.to_dict(), "right": self.right.to_dict()}}


@dataclass
class Sum(Rank):
    """Summation of multiple ranks"""
    ranks: List[Rank]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$sum": [r.to_dict() for r in self.ranks]}


@dataclass
class Val(Rank):
    """Constant rank value"""
    value: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$val": self.value}

class SelectField(Enum):
    """Predefined field types for Select"""
    DOCUMENT = "#document"
    EMBEDDING = "#embedding"
    METADATA = "#metadata"
    SCORE = "#score"


@dataclass
class Select:
    """Selection configuration for search results
    
    Fields can be:
    - SelectField.DOCUMENT - Select document field
    - SelectField.EMBEDDING - Select embedding field  
    - SelectField.METADATA - Select all metadata
    - SelectField.SCORE - Select score field
    - Any other string - Select specific metadata property
    """
    fields: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Select to a dictionary for JSON serialization"""
        return {"fields": list(self.fields)}
