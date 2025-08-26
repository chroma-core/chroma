from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Any

from chromadb.api.types import Embeddings, IDs, Include, SparseVector
from chromadb.types import (
    Collection,
    RequestVersionContext,
    Segment,
    Where,
    WhereDocument,
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


@dataclass
class Filter:
    user_ids: Optional[IDs] = None
    where: Optional[Where] = None
    where_document: Optional[WhereDocument] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Filter to a dictionary for JSON serialization"""
        result = {}
        if self.user_ids is not None:
            result["query_ids"] = self.user_ids
        if self.where is not None:
            result["where_clause"] = self.where # type: ignore[assignment]
        return result


@dataclass
class KNN:
    embeddings: Embeddings
    fetch: int


@dataclass
class Limit:
    skip: int = 0
    fetch: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Limit to a dictionary for JSON serialization"""
        result = {"skip": self.skip}
        if self.fetch is not None:
            result["fetch"] = self.fetch
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


# Score expression types for hybrid search
@dataclass
class Score:
    """Base class for Score expressions (algebraic data type)"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Score expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")


@dataclass
class Abs(Score):
    """Absolute value of a score"""
    score: Score
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$abs": self.score.to_dict()}


@dataclass
class Div(Score):
    """Division of two scores"""
    left: Score
    right: Score
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$div": {"left": self.left.to_dict(), "right": self.right.to_dict()}}


@dataclass
class Exp(Score):
    """Exponentiation of a score"""
    score: Score
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$exp": self.score.to_dict()}


@dataclass
class Log(Score):
    """Logarithm of a score"""
    score: Score
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$log": self.score.to_dict()}


@dataclass
class Max(Score):
    """Maximum of multiple scores"""
    scores: List[Score]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$max": [s.to_dict() for s in self.scores]}


@dataclass
class Min(Score):
    """Minimum of multiple scores"""
    scores: List[Score]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$min": [s.to_dict() for s in self.scores]}


@dataclass
class Mul(Score):
    """Multiplication of multiple scores"""
    scores: List[Score]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$mul": [s.to_dict() for s in self.scores]}


@dataclass
class RankScore(Score):
    """Score based on ranking"""
    source: 'Rank'
    default: Optional[float] = None
    ordinal: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        result = {"source": self.source.to_dict()}
        if self.default is not None:
            result["default"] = self.default # type: ignore[assignment]
        if self.ordinal:
            result["ordinal"] = self.ordinal # type: ignore[assignment]
        return {"$rank": result}


@dataclass
class Sub(Score):
    """Subtraction of two scores"""
    left: Score
    right: Score
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$sub": {"left": self.left.to_dict(), "right": self.right.to_dict()}}


@dataclass
class Sum(Score):
    """Summation of multiple scores"""
    scores: List[Score]
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$sum": [s.to_dict() for s in self.scores]}


@dataclass
class Val(Score):
    """Constant score value"""
    value: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {"$val": self.value}


# Rank expression types for KNN search
@dataclass
class Rank:
    """Base class for Rank expressions"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Rank expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")


@dataclass
class DenseKnn(Rank):
    """Dense KNN ranking"""
    embedding: List[float]
    key: str = "$chroma_embedding"
    limit: int = 1024
    
    def to_dict(self) -> Dict[str, Any]:
        result = {"embedding": self.embedding}
        if self.key != "$chroma_embedding":
            result["key"] = self.key # type: ignore[assignment]
        if self.limit != 1024:
            result["limit"] = self.limit # type: ignore[assignment]
        return {"$dense-knn": result}


@dataclass
class SparseKnn(Rank):
    """Sparse KNN ranking"""
    embedding: SparseVector  # Sparse vector with indices and values
    key: str  # No default for sparse KNN
    limit: int = 1024
    
    def to_dict(self) -> Dict[str, Any]:
        # Convert SparseVector to the format expected by Rust API
        result = {"embedding": self.embedding, "key": self.key}
        if self.limit != 1024:
            result["limit"] = self.limit # type: ignore[assignment]
        return {"$sparse-knn": result}


@dataclass
class Project:
    """Projection configuration for search results
    
    Fields can be:
    - "$document" - Project document field
    - "$embedding" - Project embedding field  
    - "$metadata" - Project all metadata
    - "$score" - Project score field
    - Any other string - Project specific metadata property
    """
    fields: Set[str] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Project to a dictionary for JSON serialization"""
        return {"fields": list(self.fields)}
