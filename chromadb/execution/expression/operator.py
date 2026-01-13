from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Any, Union, cast

import numpy as np
from numpy.typing import NDArray
from chromadb.api.types import (
    Embeddings,
    IDs,
    Include,
    OneOrMany,
    SparseVector,
    TYPE_KEY,
    SPARSE_VECTOR_TYPE_VALUE,
    maybe_cast_one_to_many,
    normalize_embeddings,
    validate_embeddings,
)
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

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Where":
        """Create Where expression from dictionary.

        Supports MongoDB-style query operators:
        - {"field": "value"} -> Key("field") == "value" (shorthand for equality)
        - {"field": {"$eq": value}} -> Key("field") == value
        - {"field": {"$ne": value}} -> Key("field") != value
        - {"field": {"$gt": value}} -> Key("field") > value
        - {"field": {"$gte": value}} -> Key("field") >= value
        - {"field": {"$lt": value}} -> Key("field") < value
        - {"field": {"$lte": value}} -> Key("field") <= value
        - {"field": {"$in": [values]}} -> Key("field").is_in([values])
        - {"field": {"$nin": [values]}} -> Key("field").not_in([values])
        - {"field": {"$contains": "text"}} -> Key("field").contains("text")
        - {"field": {"$not_contains": "text"}} -> Key("field").not_contains("text")
        - {"field": {"$regex": "pattern"}} -> Key("field").regex("pattern")
        - {"field": {"$not_regex": "pattern"}} -> Key("field").not_regex("pattern")
        - {"$and": [conditions]} -> condition1 & condition2 & ...
        - {"$or": [conditions]} -> condition1 | condition2 | ...
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for Where, got {type(data).__name__}")

        if not data:
            raise ValueError("Where dict cannot be empty")

        # Handle logical operators
        if "$and" in data:
            if not isinstance(data["$and"], list):
                raise TypeError(
                    f"$and must be a list, got {type(data['$and']).__name__}"
                )
            if len(data["$and"]) == 0:
                raise ValueError("$and requires at least one condition")
            if len(data) > 1:
                raise ValueError(
                    "$and cannot be combined with other fields in the same dict"
                )

            conditions = [Where.from_dict(c) for c in data["$and"]]
            if len(conditions) == 1:
                return conditions[0]
            result = conditions[0]
            for c in conditions[1:]:
                result = result & c
            return result

        elif "$or" in data:
            if not isinstance(data["$or"], list):
                raise TypeError(f"$or must be a list, got {type(data['$or']).__name__}")
            if len(data["$or"]) == 0:
                raise ValueError("$or requires at least one condition")
            if len(data) > 1:
                raise ValueError(
                    "$or cannot be combined with other fields in the same dict"
                )

            conditions = [Where.from_dict(c) for c in data["$or"]]
            if len(conditions) == 1:
                return conditions[0]
            result = conditions[0]
            for c in conditions[1:]:
                result = result | c
            return result

        else:
            # Single field condition
            if len(data) != 1:
                raise ValueError(
                    f"Where dict must contain exactly one field, got {len(data)}"
                )

            field, condition = next(iter(data.items()))

            if not isinstance(field, str):
                raise TypeError(
                    f"Field name must be a string, got {type(field).__name__}"
                )

            if isinstance(condition, dict):
                # Operator-based condition
                if not condition:
                    raise ValueError(
                        f"Operator dict for field '{field}' cannot be empty"
                    )
                if len(condition) != 1:
                    raise ValueError(
                        f"Operator dict for field '{field}' must contain exactly one operator"
                    )

                op, value = next(iter(condition.items()))

                if op == "$eq":
                    return Key(field) == value
                elif op == "$ne":
                    return Key(field) != value
                elif op == "$gt":
                    return Key(field) > value
                elif op == "$gte":
                    return Key(field) >= value
                elif op == "$lt":
                    return Key(field) < value
                elif op == "$lte":
                    return Key(field) <= value
                elif op == "$in":
                    if not isinstance(value, list):
                        raise TypeError(
                            f"$in requires a list, got {type(value).__name__}"
                        )
                    return Key(field).is_in(value)
                elif op == "$nin":
                    if not isinstance(value, list):
                        raise TypeError(
                            f"$nin requires a list, got {type(value).__name__}"
                        )
                    return Key(field).not_in(value)
                elif op == "$contains":
                    if not isinstance(value, str):
                        raise TypeError(
                            f"$contains requires a string, got {type(value).__name__}"
                        )
                    return Key(field).contains(value)
                elif op == "$not_contains":
                    if not isinstance(value, str):
                        raise TypeError(
                            f"$not_contains requires a string, got {type(value).__name__}"
                        )
                    return Key(field).not_contains(value)
                elif op == "$regex":
                    if not isinstance(value, str):
                        raise TypeError(
                            f"$regex requires a string pattern, got {type(value).__name__}"
                        )
                    return Key(field).regex(value)
                elif op == "$not_regex":
                    if not isinstance(value, str):
                        raise TypeError(
                            f"$not_regex requires a string pattern, got {type(value).__name__}"
                        )
                    return Key(field).not_regex(value)
                else:
                    raise ValueError(f"Unknown operator: {op}")
            else:
                # Direct value is shorthand for equality
                return Key(field) == condition

    def __and__(self, other: "Where") -> "And":
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

    def __or__(self, other: "Where") -> "Or":
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

    The Key class allows for readable field references using either:
    1. Predefined constants for special fields: K.EMBEDDING, K.DOCUMENT, K.SCORE, etc.
    2. String literals with # prefix for special fields: Key("#embedding")
    3. Metadata field names without # prefix: Key("my_metadata_field")

    Predefined field constants (special fields with # prefix):
        Key.ID - ID field (equivalent to Key("#id"))
        Key.DOCUMENT - Document field (equivalent to Key("#document"))
        Key.EMBEDDING - Embedding field (equivalent to Key("#embedding"))
        Key.METADATA - Metadata field (equivalent to Key("#metadata"))
        Key.SCORE - Score field (equivalent to Key("#score"))

    Note: K is an alias for Key, so you can use K.DOCUMENT or Key.DOCUMENT interchangeably.

    Examples:
        # Using predefined keys with K alias for special fields
        from chromadb.execution.expression import K
        K.DOCUMENT.contains("search text")  # Searches document field

        # Custom metadata field names (without # prefix)
        K("status") == "active"  # Metadata field named "status"
        K("category").is_in(["science", "tech"])  # Metadata field named "category"
        K("sparse_embedding")  # Example: metadata field (could store anything)

        # Using with Knn for different fields
        Knn(query=[0.1, 0.2])  # Default: searches "#embedding"
        Knn(query=[0.1, 0.2], key=K.EMBEDDING)  # Explicit: searches "#embedding"
        Knn(query=sparse, key="sparse_embedding")  # Example: searches a metadata field

        # Combining conditions
        (K("status") == "active") & (K.SCORE > 0.5)
    """

    # Predefined key constants (initialized after class definition)
    ID: "Key"
    DOCUMENT: "Key"
    EMBEDDING: "Key"
    METADATA: "Key"
    SCORE: "Key"

    def __init__(self, name: str):
        self.name = name

    def __hash__(self) -> int:
        """Make Key hashable for use in sets"""
        return hash(self.name)

    # Comparison operators
    def __eq__(self, value: Any) -> Eq:  # type: ignore[override]
        """Equality: Key('field') == value"""
        return Eq(self.name, value)

    def __ne__(self, value: Any) -> Ne:  # type: ignore[override]
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

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Limit":
        """Create Limit from dictionary.

        Examples:
        - {"offset": 10} -> Limit(offset=10)
        - {"offset": 10, "limit": 20} -> Limit(offset=10, limit=20)
        - {"limit": 20} -> Limit(offset=0, limit=20)
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for Limit, got {type(data).__name__}")

        offset = data.get("offset", 0)
        if not isinstance(offset, int):
            raise TypeError(
                f"Limit offset must be an integer, got {type(offset).__name__}"
            )
        if offset < 0:
            raise ValueError(f"Limit offset must be non-negative, got {offset}")

        limit = data.get("limit")
        if limit is not None:
            if not isinstance(limit, int):
                raise TypeError(
                    f"Limit limit must be an integer, got {type(limit).__name__}"
                )
            if limit <= 0:
                raise ValueError(f"Limit limit must be positive, got {limit}")

        # Check for unexpected keys
        allowed_keys = {"offset", "limit"}
        unexpected_keys = set(data.keys()) - allowed_keys
        if unexpected_keys:
            raise ValueError(f"Unexpected keys in Limit dict: {unexpected_keys}")

        return Limit(offset=offset, limit=limit)


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
        return includes  # type: ignore[return-value]


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

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Rank":
        """Create Rank expression from dictionary.

        Supports operators:
        - {"$val": number} -> Val(number)
        - {"$knn": {...}} -> Knn(...)
        - {"$sum": [ranks]} -> rank1 + rank2 + ...
        - {"$sub": {"left": ..., "right": ...}} -> left - right
        - {"$mul": [ranks]} -> rank1 * rank2 * ...
        - {"$div": {"left": ..., "right": ...}} -> left / right
        - {"$abs": rank} -> abs(rank)
        - {"$exp": rank} -> rank.exp()
        - {"$log": rank} -> rank.log()
        - {"$max": [ranks]} -> rank1.max(rank2).max(rank3)...
        - {"$min": [ranks]} -> rank1.min(rank2).min(rank3)...
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for Rank, got {type(data).__name__}")

        if not data:
            raise ValueError("Rank dict cannot be empty")

        if len(data) != 1:
            raise ValueError(
                f"Rank dict must contain exactly one operator, got {len(data)}"
            )

        op = next(iter(data.keys()))

        if op == "$val":
            value = data["$val"]
            if not isinstance(value, (int, float)):
                raise TypeError(f"$val requires a number, got {type(value).__name__}")
            return Val(value)

        elif op == "$knn":
            knn_data = data["$knn"]
            if not isinstance(knn_data, dict):
                raise TypeError(f"$knn requires a dict, got {type(knn_data).__name__}")

            if "query" not in knn_data:
                raise ValueError("$knn requires 'query' field")

            query = knn_data["query"]

            if isinstance(query, dict):
                # SparseVector case - deserialize from transport format
                if query.get(TYPE_KEY) == SPARSE_VECTOR_TYPE_VALUE:
                    query = SparseVector.from_dict(query)
                else:
                    # Old format or invalid - try to construct directly
                    raise ValueError(
                        f"Expected dict with {TYPE_KEY}='{SPARSE_VECTOR_TYPE_VALUE}', got {query}"
                    )

            elif isinstance(query, (list, tuple, np.ndarray)):
                # Dense vector case - normalize then validate
                normalized = normalize_embeddings(query)
                if not normalized or len(normalized) > 1:
                    raise ValueError("$knn requires exactly one query embedding")

                # Validate the normalized version
                validate_embeddings(normalized)

                query = normalized[0]

            else:
                raise TypeError(
                    f"$knn query must be a list, numpy array, or SparseVector dict, got {type(query).__name__}"
                )

            key = knn_data.get("key", "#embedding")
            if not isinstance(key, str):
                raise TypeError(f"$knn key must be a string, got {type(key).__name__}")

            limit = knn_data.get("limit", 16)
            if not isinstance(limit, int):
                raise TypeError(
                    f"$knn limit must be an integer, got {type(limit).__name__}"
                )
            if limit <= 0:
                raise ValueError(f"$knn limit must be positive, got {limit}")

            return_rank = knn_data.get("return_rank", False)
            if not isinstance(return_rank, bool):
                raise TypeError(
                    f"$knn return_rank must be a boolean, got {type(return_rank).__name__}"
                )

            return Knn(
                query=query,
                key=key,
                limit=limit,
                default=knn_data.get("default"),
                return_rank=return_rank,
            )

        elif op == "$sum":
            ranks_data = data["$sum"]
            if not isinstance(ranks_data, (list, tuple)):
                raise TypeError(
                    f"$sum requires a list, got {type(ranks_data).__name__}"
                )
            if len(ranks_data) < 2:
                raise ValueError(
                    f"$sum requires at least 2 ranks, got {len(ranks_data)}"
                )

            ranks = [Rank.from_dict(r) for r in ranks_data]
            result = ranks[0]
            for r in ranks[1:]:
                result = result + r
            return result

        elif op == "$sub":
            sub_data = data["$sub"]
            if not isinstance(sub_data, dict):
                raise TypeError(
                    f"$sub requires a dict with 'left' and 'right', got {type(sub_data).__name__}"
                )
            if "left" not in sub_data or "right" not in sub_data:
                raise ValueError("$sub requires 'left' and 'right' fields")

            left = Rank.from_dict(sub_data["left"])
            right = Rank.from_dict(sub_data["right"])
            return left - right

        elif op == "$mul":
            ranks_data = data["$mul"]
            if not isinstance(ranks_data, (list, tuple)):
                raise TypeError(
                    f"$mul requires a list, got {type(ranks_data).__name__}"
                )
            if len(ranks_data) < 2:
                raise ValueError(
                    f"$mul requires at least 2 ranks, got {len(ranks_data)}"
                )

            ranks = [Rank.from_dict(r) for r in ranks_data]
            result = ranks[0]
            for r in ranks[1:]:
                result = result * r
            return result

        elif op == "$div":
            div_data = data["$div"]
            if not isinstance(div_data, dict):
                raise TypeError(
                    f"$div requires a dict with 'left' and 'right', got {type(div_data).__name__}"
                )
            if "left" not in div_data or "right" not in div_data:
                raise ValueError("$div requires 'left' and 'right' fields")

            left = Rank.from_dict(div_data["left"])
            right = Rank.from_dict(div_data["right"])
            return left / right

        elif op == "$abs":
            child_data = data["$abs"]
            if not isinstance(child_data, dict):
                raise TypeError(
                    f"$abs requires a rank dict, got {type(child_data).__name__}"
                )
            return abs(Rank.from_dict(child_data))

        elif op == "$exp":
            child_data = data["$exp"]
            if not isinstance(child_data, dict):
                raise TypeError(
                    f"$exp requires a rank dict, got {type(child_data).__name__}"
                )
            return Rank.from_dict(child_data).exp()

        elif op == "$log":
            child_data = data["$log"]
            if not isinstance(child_data, dict):
                raise TypeError(
                    f"$log requires a rank dict, got {type(child_data).__name__}"
                )
            return Rank.from_dict(child_data).log()

        elif op == "$max":
            ranks_data = data["$max"]
            if not isinstance(ranks_data, (list, tuple)):
                raise TypeError(
                    f"$max requires a list, got {type(ranks_data).__name__}"
                )
            if len(ranks_data) < 2:
                raise ValueError(
                    f"$max requires at least 2 ranks, got {len(ranks_data)}"
                )

            ranks = [Rank.from_dict(r) for r in ranks_data]
            result = ranks[0]
            for r in ranks[1:]:
                result = result.max(r)
            return result

        elif op == "$min":
            ranks_data = data["$min"]
            if not isinstance(ranks_data, (list, tuple)):
                raise TypeError(
                    f"$min requires a list, got {type(ranks_data).__name__}"
                )
            if len(ranks_data) < 2:
                raise ValueError(
                    f"$min requires at least 2 ranks, got {len(ranks_data)}"
                )

            ranks = [Rank.from_dict(r) for r in ranks_data]
            result = ranks[0]
            for r in ranks[1:]:
                result = result.min(r)
            return result

        else:
            raise ValueError(f"Unknown rank operator: {op}")

    # Arithmetic operators
    def __add__(self, other: Union["Rank", float, int]) -> "Sum":
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

    def __radd__(self, other: Union[float, int]) -> "Sum":
        """Right addition: value + rank"""
        return Val(other) + self

    def __sub__(self, other: Union["Rank", float, int]) -> "Sub":
        """Subtraction: rank1 - rank2 or rank - value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        return Sub(self, other_rank)

    def __rsub__(self, other: Union[float, int]) -> "Sub":
        """Right subtraction: value - rank"""
        return Sub(Val(other), self)

    def __mul__(self, other: Union["Rank", float, int]) -> "Mul":
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

    def __rmul__(self, other: Union[float, int]) -> "Mul":
        """Right multiplication: value * rank"""
        return Val(other) * self

    def __truediv__(self, other: Union["Rank", float, int]) -> "Div":
        """Division: rank1 / rank2 or rank / value"""
        other_rank = Val(other) if isinstance(other, (int, float)) else other
        return Div(self, other_rank)

    def __rtruediv__(self, other: Union[float, int]) -> "Div":
        """Right division: value / rank"""
        return Div(Val(other), self)

    def __neg__(self) -> "Mul":
        """Negation: -rank (equivalent to -1 * rank)"""
        return Mul([Val(-1), self])

    def __abs__(self) -> "Abs":
        """Absolute value: abs(rank)"""
        return Abs(self)

    def abs(self) -> "Abs":
        """Absolute value builder: rank.abs()"""
        return Abs(self)

    # Builder methods for functions
    def exp(self) -> "Exp":
        """Exponential: e^rank"""
        return Exp(self)

    def log(self) -> "Log":
        """Natural logarithm: ln(rank)"""
        return Log(self)

    def max(self, other: Union["Rank", float, int]) -> "Max":
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

    def min(self, other: Union["Rank", float, int]) -> "Min":
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
        query: The query for KNN search. Can be:
               - A string (will be automatically embedded using the collection's embedding function)
               - A dense vector (list or numpy array)
               - A sparse vector (SparseVector dict)
        key: The embedding key to search against. Can be:
             - Key.EMBEDDING (default) - searches the main embedding field
             - A metadata field name (e.g., "my_custom_field") - searches that metadata field
        limit: Maximum number of results to consider (default: 16)
        default: Default score for records not in KNN results (default: None)
        return_rank: If True, return the rank position (0, 1, 2, ...) instead of distance (default: False)

    Examples:
        # Search with string query (automatically embedded)
        Knn(query="hello world")  # Will use collection's embedding function

        # Search main embeddings with vectors (equivalent forms)
        Knn(query=[0.1, 0.2])  # Uses default key="#embedding"
        Knn(query=[0.1, 0.2], key=K.EMBEDDING)
        Knn(query=[0.1, 0.2], key="#embedding")

        # Search sparse embeddings stored in metadata with string
        Knn(query="hello world", key="custom_embedding")  # Will use schema's embedding function

        # Search sparse embeddings stored in metadata with vector
        Knn(query=my_vector, key="custom_embedding")  # Example: searches a metadata field
    """

    query: Union[
        str,
        List[float],
        SparseVector,
        "NDArray[np.float32]",
        "NDArray[np.float64]",
        "NDArray[np.int32]",
    ]
    key: Union[Key, str] = K.EMBEDDING
    limit: int = 16
    default: Optional[float] = None
    return_rank: bool = False

    def to_dict(self) -> Dict[str, Any]:
        # Convert to transport format
        query_value = self.query
        if isinstance(query_value, SparseVector):
            # Convert SparseVector dataclass to transport dict
            query_value = query_value.to_dict()
        elif isinstance(query_value, np.ndarray):
            # Convert numpy array to list
            query_value = query_value.tolist()

        key_value = self.key
        if isinstance(key_value, Key):
            key_value = key_value.name

        # Build result dict - only include non-default values to keep JSON clean
        result = {"query": query_value, "key": key_value, "limit": self.limit}

        # Only include optional fields if they're set to non-default values
        if self.default is not None:
            result["default"] = self.default  # type: ignore[assignment]
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
class Rrf(Rank):
    """Reciprocal Rank Fusion for combining multiple ranking strategies.

    RRF formula: score = -sum(weight_i / (k + rank_i)) for each ranking strategy
    The negative is used because RRF produces higher scores for better results,
    but Chroma uses ascending order (lower scores = better results).

    Args:
        ranks: List of Rank expressions to fuse (must have at least one)
        k: Smoothing constant (default: 60, standard in literature)
        weights: Optional weights for each ranking strategy. If not provided,
                all ranks are weighted equally (weight=1.0 each).
        normalize: If True, normalize weights to sum to 1.0 (default: False).
                  When False, weights are used as-is for relative importance.
                  When True, weights are scaled so they sum to 1.0.

    Examples:
        # Note: metadata fields (like "sparse_embedding" below) are user-defined and can store any data.
        # The field name is just an example - use whatever name matches your metadata structure.
        # Basic RRF combining KNN rankings (equal weight)
        Rrf([
            Knn(query=[0.1, 0.2], return_rank=True),
            Knn(query=another_vector, key="custom_embedding", return_rank=True)  # Example metadata field
        ])

        # Weighted RRF with relative weights (not normalized)
        Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=another_vector, key="custom_embedding", return_rank=True)  # Example metadata field
            weights=[2.0, 1.0],  # First ranking is 2x more important
            k=100
        )

        # Weighted RRF with normalized weights
        Rrf(
            ranks=[
                Knn(query=[0.1, 0.2], return_rank=True),
                Knn(query=another_vector, key="custom_embedding", return_rank=True)  # Example metadata field
            ],
            weights=[3.0, 1.0],  # Will be normalized to [0.75, 0.25]
            normalize=True,
            k=100
        )
    """

    ranks: List[Rank]
    k: int = 60
    weights: Optional[List[float]] = None
    normalize: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert RRF to a composition of existing expression operators.

        Builds: -sum(weight_i / (k + rank_i)) for each rank
        Using Python's overloaded operators for cleaner code.
        """
        # Validate RRF parameters
        if not self.ranks:
            raise ValueError("RRF requires at least one rank")
        if self.k <= 0:
            raise ValueError(f"k must be positive, got {self.k}")

        # Validate weights if provided
        if self.weights is not None:
            if len(self.weights) != len(self.ranks):
                raise ValueError(
                    f"Number of weights ({len(self.weights)}) must match number of ranks ({len(self.ranks)})"
                )
            if any(w < 0.0 for w in self.weights):
                raise ValueError("All weights must be non-negative")

        # Populate weights with 1.0 if not provided
        weights = self.weights if self.weights else [1.0] * len(self.ranks)

        # Normalize weights if requested
        if self.normalize:
            weight_sum = sum(weights)
            if weight_sum == 0:
                raise ValueError("Sum of weights must be positive when normalize=True")
            weights = [w / weight_sum for w in weights]

        # Zip weights with ranks and build terms: weight / (k + rank)
        terms = [w / (self.k + rank) for w, rank in zip(weights, self.ranks)]

        # Sum all terms - guaranteed to have at least one
        rrf_sum: Rank = terms[0]
        for term in terms[1:]:
            rrf_sum = rrf_sum + term

        # Negate (RRF gives higher scores for better, Chroma needs lower for better)
        return (-rrf_sum).to_dict()


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

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Select":
        """Create Select from dictionary.

        Examples:
        - {"keys": ["#document", "#score"]} -> Select(keys={Key.DOCUMENT, Key.SCORE})
        - {"keys": ["title", "author"]} -> Select(keys={"title", "author"})
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for Select, got {type(data).__name__}")

        keys = data.get("keys", [])
        if not isinstance(keys, (list, tuple, set)):
            raise TypeError(
                f"Select keys must be a list/tuple/set, got {type(keys).__name__}"
            )

        # Validate and convert each key
        key_list = []
        for k in keys:
            if not isinstance(k, str):
                raise TypeError(f"Select key must be a string, got {type(k).__name__}")

            # Map special keys to Key instances
            if k == "#id":
                key_list.append(Key.ID)
            elif k == "#document":
                key_list.append(Key.DOCUMENT)
            elif k == "#embedding":
                key_list.append(Key.EMBEDDING)
            elif k == "#metadata":
                key_list.append(Key.METADATA)
            elif k == "#score":
                key_list.append(Key.SCORE)
            else:
                # Regular metadata field
                key_list.append(Key(k))

        # Check for unexpected keys in dict
        allowed_keys = {"keys"}
        unexpected_keys = set(data.keys()) - allowed_keys
        if unexpected_keys:
            raise ValueError(f"Unexpected keys in Select dict: {unexpected_keys}")

        # Convert to set while preserving the Key instances
        return Select(keys=set(key_list))


# GroupBy and Aggregate types for grouping search results


def _keys_to_strings(keys: OneOrMany[Union[Key, str]]) -> List[str]:
    """Convert OneOrMany[Key|str] to List[str] for serialization."""
    keys_list = cast(List[Union[Key, str]], maybe_cast_one_to_many(keys))
    return [k.name if isinstance(k, Key) else k for k in keys_list]


def _strings_to_keys(keys: Union[List[Any], tuple[Any, ...]]) -> List[Union[Key, str]]:
    """Convert List[str] to List[Key] for deserialization."""
    return [Key(k) if isinstance(k, str) else k for k in keys]


def _parse_k_aggregate(
    op: str, data: Dict[str, Any]
) -> tuple[List[Union[Key, str]], int]:
    """Parse common fields for MinK/MaxK from dict.

    Args:
        op: The operator name (e.g., "$min_k" or "$max_k")
        data: The dict containing the operator

    Returns:
        Tuple of (keys, k) where keys is List[Union[Key, str]] and k is int

    Raises:
        TypeError: If data types are invalid
        ValueError: If required fields are missing or invalid
    """
    agg_data = data[op]
    if not isinstance(agg_data, dict):
        raise TypeError(f"{op} requires a dict, got {type(agg_data).__name__}")
    if "keys" not in agg_data:
        raise ValueError(f"{op} requires 'keys' field")
    if "k" not in agg_data:
        raise ValueError(f"{op} requires 'k' field")

    keys = agg_data["keys"]
    if not isinstance(keys, (list, tuple)):
        raise TypeError(f"{op} keys must be a list, got {type(keys).__name__}")
    if not keys:
        raise ValueError(f"{op} keys cannot be empty")

    k = agg_data["k"]
    if not isinstance(k, int):
        raise TypeError(f"{op} k must be an integer, got {type(k).__name__}")
    if k <= 0:
        raise ValueError(f"{op} k must be positive, got {k}")

    return _strings_to_keys(keys), k


@dataclass
class Aggregate:
    """Base class for aggregation expressions within groups.

    Aggregations determine which records to keep from each group:
    - MinK: Keep k records with minimum values (ascending order)
    - MaxK: Keep k records with maximum values (descending order)

    Examples:
        # Keep top 3 by score per group (single key)
        MinK(keys=Key.SCORE, k=3)

        # Keep top 5 by priority, then score as tiebreaker (multiple keys)
        MinK(keys=[Key("priority"), Key.SCORE], k=5)

        # Keep bottom 2 by score per group
        MaxK(keys=Key.SCORE, k=2)
    """

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Aggregate expression to a dictionary for JSON serialization"""
        raise NotImplementedError("Subclasses must implement to_dict()")

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Aggregate":
        """Create Aggregate expression from dictionary.

        Supports:
        - {"$min_k": {"keys": [...], "k": n}} -> MinK(keys=[...], k=n)
        - {"$max_k": {"keys": [...], "k": n}} -> MaxK(keys=[...], k=n)
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for Aggregate, got {type(data).__name__}")

        if not data:
            raise ValueError("Aggregate dict cannot be empty")

        if len(data) != 1:
            raise ValueError(
                f"Aggregate dict must contain exactly one operator, got {len(data)}"
            )

        op = next(iter(data.keys()))

        if op == "$min_k":
            keys, k = _parse_k_aggregate(op, data)
            return MinK(keys=keys, k=k)
        elif op == "$max_k":
            keys, k = _parse_k_aggregate(op, data)
            return MaxK(keys=keys, k=k)
        else:
            raise ValueError(f"Unknown aggregate operator: {op}")


@dataclass
class MinK(Aggregate):
    """Keep k records with minimum aggregate key values per group"""

    keys: OneOrMany[Union[Key, str]]
    k: int

    def to_dict(self) -> Dict[str, Any]:
        return {"$min_k": {"keys": _keys_to_strings(self.keys), "k": self.k}}


@dataclass
class MaxK(Aggregate):
    """Keep k records with maximum aggregate key values per group"""

    keys: OneOrMany[Union[Key, str]]
    k: int

    def to_dict(self) -> Dict[str, Any]:
        return {"$max_k": {"keys": _keys_to_strings(self.keys), "k": self.k}}


@dataclass
class GroupBy:
    """Group results by metadata keys and aggregate within each group.

    Groups search results by one or more metadata fields, then applies an
    aggregation (MinK or MaxK) to select records within each group.
    The final output is flattened and sorted by score.

    Args:
        keys: Metadata key(s) to group by. Can be a single key or a list of keys.
              E.g., Key("category") or [Key("category"), Key("author")]
        aggregate: Aggregation to apply within each group (MinK or MaxK)

    Note: Both keys and aggregate must be specified together.

    Examples:
        # Top 3 documents per category (single key)
        GroupBy(
            keys=Key("category"),
            aggregate=MinK(keys=Key.SCORE, k=3)
        )

        # Top 2 per (year, category) combination (multiple keys)
        GroupBy(
            keys=[Key("year"), Key("category")],
            aggregate=MinK(keys=Key.SCORE, k=2)
        )

        # Top 1 per category by priority, score as tiebreaker
        GroupBy(
            keys=Key("category"),
            aggregate=MinK(keys=[Key("priority"), Key.SCORE], k=1)
        )
    """

    keys: OneOrMany[Union[Key, str]] = field(default_factory=list)
    aggregate: Optional[Aggregate] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert the GroupBy to a dictionary for JSON serialization"""
        # Default GroupBy (no keys, no aggregate) serializes to {}
        if not self.keys or self.aggregate is None:
            return {}
        result: Dict[str, Any] = {"keys": _keys_to_strings(self.keys)}
        result["aggregate"] = self.aggregate.to_dict()
        return result

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "GroupBy":
        """Create GroupBy from dictionary.

        Examples:
        - {} -> GroupBy() (default, no grouping)
        - {"keys": ["category"], "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}}
        """
        if not isinstance(data, dict):
            raise TypeError(f"Expected dict for GroupBy, got {type(data).__name__}")

        # Empty dict returns default GroupBy (no grouping)
        if not data:
            return GroupBy()

        # Non-empty dict requires keys and aggregate
        if "keys" not in data:
            raise ValueError("GroupBy requires 'keys' field")
        if "aggregate" not in data:
            raise ValueError("GroupBy requires 'aggregate' field")

        keys = data["keys"]
        if not isinstance(keys, (list, tuple)):
            raise TypeError(f"GroupBy keys must be a list, got {type(keys).__name__}")
        if not keys:
            raise ValueError("GroupBy keys cannot be empty")

        aggregate_data = data["aggregate"]
        if not isinstance(aggregate_data, dict):
            raise TypeError(
                f"GroupBy aggregate must be a dict, got {type(aggregate_data).__name__}"
            )
        aggregate = Aggregate.from_dict(aggregate_data)

        return GroupBy(keys=_strings_to_keys(keys), aggregate=aggregate)
