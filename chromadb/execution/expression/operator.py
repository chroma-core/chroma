from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Set, Any, Union

import numpy as np
from numpy.typing import NDArray
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
    """Base class for Where expressions (algebraic data type).
    
    Supports logical operators for combining conditions:
        - AND: where1 & where2
        - OR: where1 | where2
    
    Examples:
        # Simple conditions
        where1 = Key("status") == "active"
        where2 = Key("score") > 0.5
        
        # Combining with AND
        combined_and = where1 & where2
        
        # Combining with OR
        combined_or = where1 | where2
        
        # Complex expressions
        complex_where = (Key("status") == "active") & ((Key("score") > 0.5) | (Key("priority") == "high"))
    """
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Where expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")
    
    def __and__(self, other: 'Where') -> 'And':
        """Overload & operator for AND"""
        # If self is already an And, extend it
        if isinstance(self, And):
            # If other is also And, combine all conditions
            if isinstance(other, And):
                return And(self.conditions + other.conditions)
            return And(self.conditions + [other])
        # If other is And, prepend self to it
        elif isinstance(other, And):
            return And([self] + other.conditions)
        # Create new And with both conditions
        return And([self, other])
    
    def __or__(self, other: 'Where') -> 'Or':
        """Overload | operator for OR"""
        # If self is already an Or, extend it
        if isinstance(self, Or):
            # If other is also Or, combine all conditions
            if isinstance(other, Or):
                return Or(self.conditions + other.conditions)
            return Or(self.conditions + [other])
        # If other is Or, prepend self to it
        elif isinstance(other, Or):
            return Or([self] + other.conditions)
        # Create new Or with both conditions
        return Or([self, other])


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
    key: str
    content: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$contains": self.content}}


@dataclass
class NotContains(Where):
    """Not contains comparison for document content"""
    key: str
    content: str
    
    def to_dict(self) -> Dict[str, Any]:
        return {self.key: {"$not_contains": self.content}}


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


# Field proxy for building Where conditions
class Key:
    """Field proxy for building Where conditions with operator overloading.
    
    Predefined field constants:
        Key.ID - ID field (equivalent to Key("#id"))
        Key.DOCUMENT - Document field (equivalent to Key("#document"))
        Key.EMBEDDING - Embedding field (equivalent to Key("#embedding"))
        Key.METADATA - Metadata field (equivalent to Key("#metadata"))
        Key.SCORE - Score field (equivalent to Key("#score"))
    
    Note: K is an alias for Key, so you can use K.DOCUMENT or Key.DOCUMENT interchangeably.
    
    Examples:
        # Using predefined keys with K alias
        from chromadb.execution.expression import K
        K.DOCUMENT.contains("search text")
        
        # Custom field names
        K("status") == "active"
        K("category").is_in(["science", "tech"])
        
        # Combining conditions
        (K("status") == "active") & (K.SCORE > 0.5)
    """
    
    # Predefined key constants (initialized after class definition)
    ID: 'Key'
    DOCUMENT: 'Key'
    EMBEDDING: 'Key'  
    METADATA: 'Key'
    SCORE: 'Key'
    
    def __init__(self, name: str):
        self.name = name
    
    def __eq__(self, other: Any) -> Union[Eq, bool]: # type: ignore[override]
        """Equality operator - can be used for Key comparison or Where condition creation"""
        if isinstance(other, Key):
            return self.name == other.name
        else:
            # Create Where condition
            return Eq(self.name, other)
    
    def __hash__(self) -> int:
        """Make Key hashable for use in sets"""
        return hash(self.name)
    
    # Comparison operators  
    def __ne__(self, value: Any) -> Ne: # type: ignore[override]
        """Not equal: Key('field') != value"""
        return Ne(self.name, value)
    
    def __gt__(self, value: Any) -> Gt:
        """Greater than: Key('field') > value"""
        return Gt(self.name, value)
    
    def __ge__(self, value: Any) -> Gte:
        """Greater than or equal: Key('field') >= value"""
        return Gte(self.name, value)
    
    def __lt__(self, value: Any) -> Lt:
        """Less than: Key('field') < value"""
        return Lt(self.name, value)
    
    def __le__(self, value: Any) -> Lte:
        """Less than or equal: Key('field') <= value"""
        return Lte(self.name, value)
    
    # Builder methods for operations without operators
    def is_in(self, values: List[Any]) -> In:
        """Check if field value is in list: Key('field').is_in(['a', 'b'])"""
        return In(self.name, values)
    
    def not_in(self, values: List[Any]) -> Nin:
        """Check if field value is not in list: Key('field').not_in(['a', 'b'])"""
        return Nin(self.name, values)
    
    def regex(self, pattern: str) -> Regex:
        """Match field against regex: Key('field').regex('^pattern')"""
        return Regex(self.name, pattern)
    
    def not_regex(self, pattern: str) -> NotRegex:
        """Field should not match regex: Key('field').not_regex('^pattern')"""
        return NotRegex(self.name, pattern)
    
    def contains(self, content: str) -> Contains:
        """Check if field contains text: Key('field').contains('text')"""
        return Contains(self.name, content)
    
    def not_contains(self, content: str) -> NotContains:
        """Check if field doesn't contain text: Key('field').not_contains('text')"""
        return NotContains(self.name, content)


# Initialize predefined key constants
Key.ID = Key("#id")
Key.DOCUMENT = Key("#document")
Key.EMBEDDING = Key("#embedding")
Key.METADATA = Key("#metadata")
Key.SCORE = Key("#score")

# Alias for Key
K = Key


@dataclass
class Filter:
    user_ids: Optional[IDs] = None
    where: Optional[Any] = None  # Old Where type from chromadb.types
    where_document: Optional[Any] = None  # Old WhereDocument type
    

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
    """Base class for Rank expressions (algebraic data type).
    
    Supports arithmetic operations for combining rank expressions:
        - Addition: rank1 + rank2, rank + 0.5
        - Subtraction: rank1 - rank2, rank - 0.5
        - Multiplication: rank1 * rank2, rank * 0.8
        - Division: rank1 / rank2, rank / 2.0
        - Negation: -rank
        - Absolute value: abs(rank)
    
    Supports mathematical functions:
        - Exponential: rank.exp()
        - Logarithm: rank.log()
        - Maximum: rank.max(other)
        - Minimum: rank.min(other)
    
    Examples:
        # Weighted combination
        Knn(query=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2
        
        # Normalization
        Knn(query=[0.1, 0.2]) / Val(10.0)
        
        # Clamping
        Knn(query=[0.1, 0.2]).min(1.0).max(0.0)
    """
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Score expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")
    
    # Arithmetic operators
    def __add__(self, other: Union['Rank', float, int]) -> 'Sum':
        """Addition: rank1 + rank2 or rank + value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        # Flatten if already Sum
        if isinstance(self, Sum):
            if isinstance(other_rank, Sum):
                return Sum(self.ranks + other_rank.ranks)
            return Sum(self.ranks + [other_rank])
        elif isinstance(other_rank, Sum):
            return Sum([self] + other_rank.ranks)
        return Sum([self, other_rank])
    
    def __radd__(self, other: Union[float, int]) -> 'Sum':
        """Right addition: value + rank"""
        return Val(other) + self
    
    def __sub__(self, other: Union['Rank', float, int]) -> 'Sub':
        """Subtraction: rank1 - rank2 or rank - value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        return Sub(self, other_rank)
    
    def __rsub__(self, other: Union[float, int]) -> 'Sub':
        """Right subtraction: value - rank"""
        return Sub(Val(other), self)
    
    def __mul__(self, other: Union['Rank', float, int]) -> 'Mul':
        """Multiplication: rank1 * rank2 or rank * value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        # Flatten if already Mul
        if isinstance(self, Mul):
            if isinstance(other_rank, Mul):
                return Mul(self.ranks + other_rank.ranks)
            return Mul(self.ranks + [other_rank])
        elif isinstance(other_rank, Mul):
            return Mul([self] + other_rank.ranks)
        return Mul([self, other_rank])
    
    def __rmul__(self, other: Union[float, int]) -> 'Mul':
        """Right multiplication: value * rank"""
        return Val(other) * self
    
    def __truediv__(self, other: Union['Rank', float, int]) -> 'Div':
        """Division: rank1 / rank2 or rank / value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        return Div(self, other_rank)
    
    def __rtruediv__(self, other: Union[float, int]) -> 'Div':
        """Right division: value / rank"""
        return Div(Val(other), self)
    
    def __neg__(self) -> 'Mul':
        """Negation: -rank (equivalent to -1 * rank)"""
        return Mul([Val(-1), self])
    
    def __abs__(self) -> 'Abs':
        """Absolute value: abs(rank)"""
        return Abs(self)
    
    # Builder methods for functions
    def exp(self) -> 'Exp':
        """Exponential: e^rank"""
        return Exp(self)
    
    def log(self) -> 'Log':
        """Natural logarithm: ln(rank)"""
        return Log(self)
    
    def max(self, other: Union['Rank', float, int]) -> 'Max':
        """Maximum of this rank and another: rank.max(rank2)"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        
        # Flatten if already Max
        if isinstance(self, Max):
            if isinstance(other_rank, Max):
                return Max(self.ranks + other_rank.ranks)
            return Max(self.ranks + [other_rank])
        elif isinstance(other_rank, Max):
            return Max([self] + other_rank.ranks)
        return Max([self, other_rank])
    
    def min(self, other: Union['Rank', float, int]) -> 'Min':
        """Minimum of this rank and another: rank.min(rank2)"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        
        # Flatten if already Min
        if isinstance(self, Min):
            if isinstance(other_rank, Min):
                return Min(self.ranks + other_rank.ranks)
            return Min(self.ranks + [other_rank])
        elif isinstance(other_rank, Min):
            return Min([self] + other_rank.ranks)
        return Min([self, other_rank])


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
    """KNN-based ranking
    
    Args:
        query: The query vector for KNN search (dense, sparse, or numpy array)
        key: The embedding key to search against (default: "#embedding")
        limit: Maximum number of results to consider (default: 128)
        default: Default score for records not in KNN results (default: None)
        return_rank: If True, return the rank position (0, 1, 2, ...) instead of distance (default: False)
    """
    query: Union[List[float], SparseVector, "NDArray[np.float32]", "NDArray[np.float64]", "NDArray[np.int32]"]
    key: str = "#embedding"
    limit: int = 128
    default: Optional[float] = None
    return_rank: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        # Convert numpy array to list if needed
        query_value = self.query
        if isinstance(query_value, np.ndarray):
            query_value = query_value.tolist()
        
        # Build result dict - only include non-default values to keep JSON clean
        result = {
            "query": query_value,
            "key": self.key,
            "limit": self.limit
        }
        
        # Only include optional fields if they're set to non-default values
        if self.default is not None:
            result["default"] = self.default # type: ignore[assignment]
        if self.return_rank:  # Only include if True (non-default)
            result["return_rank"] = self.return_rank
        
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


@dataclass
class Select:
    """Selection configuration for search results.
    
    Fields can be:
    - Key.DOCUMENT - Select document key (equivalent to Key("#document"))
    - Key.EMBEDDING - Select embedding key (equivalent to Key("#embedding"))
    - Key.SCORE - Select score key (equivalent to Key("#score"))
    - Any other string - Select specific metadata property
    
    Note: You can use K as an alias for Key for more concise code.
    
    Examples:
        # Select predefined keys using K alias (K is shorthand for Key)
        from chromadb.execution.expression import K
        Select(keys={K.DOCUMENT, K.SCORE})
        
        # Select specific metadata properties
        Select(keys={"title", "author", "date"})
        
        # Mixed selection
        Select(keys={K.DOCUMENT, "title", "author"})
    """
    keys: Set[Union[Key, str]] = field(default_factory=set)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Select to a dictionary for JSON serialization"""
        # Convert Key objects to their string values
        key_strings = []
        for k in self.keys:
            if isinstance(k, Key):
                key_strings.append(k.name)
            else:
                key_strings.append(k)
        # Remove duplicates while preserving order
        return {"keys": list(dict.fromkeys(key_strings))}
