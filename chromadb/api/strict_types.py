"""Strict TypedDict definitions for SDK interfaces.

This module provides stricter type definitions to replace permissive Dict[str, Any]
types in the Chroma SDK, improving type safety and preventing runtime errors
from malformed dictionaries.
"""
from typing import Dict, List, Union, Optional, Any
from typing_extensions import TypedDict, Literal, NotRequired


# SparseVector types
class SparseVectorDict(TypedDict):
    """Strict type for SparseVector dictionary representation."""

    indices: List[int]
    values: List[float]
    tokens: NotRequired[Optional[List[str]]]  # Wire format uses 'tokens', mapped to 'labels'


class SparseVectorTransportDict(TypedDict):
    """Transport format for SparseVector with type tag."""

    indices: List[int]
    values: List[float]
    tokens: NotRequired[Optional[List[str]]]


# Use functional syntax to avoid name mangling with special keys
TypedSparseVectorTransportDict = TypedDict("TypedSparseVectorTransportDict", {
    "indices": List[int],
    "values": List[float],
    "tokens": Optional[List[str]],  # NotRequired not needed in functional syntax
    "#type": Literal["sparse_vector"]  # Real key name without mangling
})


# Where expression types
WhereValue = Union[str, int, float, bool]


class WhereEqDict(TypedDict):
    """Where condition with equality operator."""

    __eq: WhereValue  # Uses __eq to avoid conflicts with $eq


class WhereNeDict(TypedDict):
    """Where condition with not-equal operator."""

    __ne: WhereValue


class WhereGtDict(TypedDict):
    """Where condition with greater-than operator."""

    __gt: WhereValue


class WhereGteDict(TypedDict):
    """Where condition with greater-than-or-equal operator."""

    __gte: WhereValue


class WhereLtDict(TypedDict):
    """Where condition with less-than operator."""

    __lt: WhereValue


class WhereLteDict(TypedDict):
    """Where condition with less-than-or-equal operator."""

    __lte: WhereValue


class WhereInDict(TypedDict):
    """Where condition with $in operator."""

    __in: List[WhereValue]


class WhereNinDict(TypedDict):
    """Where condition with $nin operator."""

    __nin: List[WhereValue]


class WhereContainsDict(TypedDict):
    """Where condition with $contains operator."""

    __contains: WhereValue


class WhereNotContainsDict(TypedDict):
    """Where condition with $not_contains operator."""

    __not_contains: WhereValue


class WhereRegexDict(TypedDict):
    """Where condition with $regex operator."""

    __regex: str


class WhereNotRegexDict(TypedDict):
    """Where condition with $not_regex operator."""

    __not_regex: str


# Union of all operator dicts
WhereOperatorDict = Union[
    WhereEqDict,
    WhereNeDict,
    WhereGtDict,
    WhereGteDict,
    WhereLtDict,
    WhereLteDict,
    WhereInDict,
    WhereNinDict,
    WhereContainsDict,
    WhereNotContainsDict,
    WhereRegexDict,
    WhereNotRegexDict,
]


class WhereFieldDict(TypedDict, total=False):
    """Where condition for a single field - can be direct value or operator dict."""

    # This allows arbitrary field names with either direct values or operator dicts
    # The actual validation happens at runtime in from_dict methods


class WhereAndDict(TypedDict):
    """Where condition with $and logical operator."""

    __and: List["WhereDict"]


class WhereOrDict(TypedDict):
    """Where condition with $or logical operator."""

    __or: List["WhereDict"]


# Recursive where dict type
WhereDict = Union[WhereAndDict, WhereOrDict, Dict[str, Union[WhereValue, WhereOperatorDict]]]


# Limit types
class LimitDict(TypedDict):
    """Strict type for Limit dictionary representation."""

    offset: NotRequired[int]  # Default: 0
    limit: NotRequired[Optional[int]]  # Default: None


# Rank expression types
class ValRankDict(TypedDict):
    """Rank expression for constant value."""

    __val: Union[int, float]


class KnnRankDict(TypedDict):
    """Rank expression for KNN search."""

    query: Union[List[float], TypedSparseVectorTransportDict]
    key: NotRequired[str]  # Default: "#embedding"
    limit: NotRequired[int]  # Default: 16
    default: NotRequired[Optional[float]]  # Default: None
    return_rank: NotRequired[bool]  # Default: False


class SumRankDict(TypedDict):
    """Rank expression for summation."""

    __sum: List["RankDict"]


class SubRankDict(TypedDict):
    """Rank expression for subtraction."""

    __sub: "BinaryRankDict"


class MulRankDict(TypedDict):
    """Rank expression for multiplication."""

    __mul: List["RankDict"]


class DivRankDict(TypedDict):
    """Rank expression for division."""

    __div: "BinaryRankDict"


class AbsRankDict(TypedDict):
    """Rank expression for absolute value."""

    __abs: "RankDict"


class ExpRankDict(TypedDict):
    """Rank expression for exponential."""

    __exp: "RankDict"


class LogRankDict(TypedDict):
    """Rank expression for logarithm."""

    __log: "RankDict"


class MaxRankDict(TypedDict):
    """Rank expression for maximum."""

    __max: List["RankDict"]


class MinRankDict(TypedDict):
    """Rank expression for minimum."""

    __min: List["RankDict"]


class BinaryRankDict(TypedDict):
    """Binary operation between two ranks."""

    left: "RankDict"
    right: "RankDict"


class KnnFullRankDict(TypedDict):
    """Full KNN rank expression."""

    __knn: KnnRankDict


# Union of all rank expression types
RankDict = Union[
    ValRankDict,
    KnnFullRankDict,
    SumRankDict,
    SubRankDict,
    MulRankDict,
    DivRankDict,
    AbsRankDict,
    ExpRankDict,
    LogRankDict,
    MaxRankDict,
    MinRankDict,
]


# Select types
class SelectDict(TypedDict):
    """Strict type for Select dictionary representation."""

    keys: List[str]


# Aggregate types
class MinKAggregateDict(TypedDict):
    """MinK aggregate operation."""

    keys: List[str]
    k: int


class MaxKAggregateDict(TypedDict):
    """MaxK aggregate operation."""

    keys: List[str]
    k: int


# Use functional syntax to avoid name mangling with $ keys
MinKDict = TypedDict("MinKDict", {
    "$min_k": MinKAggregateDict
})

MaxKDict = TypedDict("MaxKDict", {
    "$max_k": MaxKAggregateDict
})


AggregateDict = Union[MinKDict, MaxKDict]


# GroupBy types
class GroupByDict(TypedDict, total=False):
    """Strict type for GroupBy dictionary representation."""

    keys: List[str]  # Required if not empty
    aggregate: AggregateDict  # Required if not empty


# CMEK types
class CmekGcpDict(TypedDict):
    """CMEK configuration for GCP."""

    gcp: str


CmekDict = CmekGcpDict


# Schema types - keeping flexible for now as Schema is complex
SchemaDict = Dict[str, Any]  # TODO: This needs more detailed typing