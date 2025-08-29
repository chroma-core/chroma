from typing import Dict, List, Mapping, Optional, Sequence, Union
from typing_extensions import Literal, TypedDict
import numpy as np
from numpy.typing import NDArray


class SparseVector(TypedDict):
    """Represents a sparse vector using parallel arrays for indices and values.
    
    Attributes:
        indices: List of dimension indices (must be non-negative integers)
        values: List of values corresponding to each index
    """
    indices: List[int]
    values: List[float]


Metadata = Mapping[str, Optional[Union[str, int, float, bool, SparseVector]]]
UpdateMetadata = Mapping[str, Union[int, float, str, bool, SparseVector, None]]
PyVector = Union[Sequence[float], Sequence[int]]
Vector = NDArray[Union[np.int32, np.float32]]  # TODO: Specify that the vector is 1D
# Metadata Query Grammar
LiteralValue = Union[str, int, float, bool]
LogicalOperator = Union[Literal["$and"], Literal["$or"]]
WhereOperator = Union[
    Literal["$gt"],
    Literal["$gte"],
    Literal["$lt"],
    Literal["$lte"],
    Literal["$ne"],
    Literal["$eq"],
]
InclusionExclusionOperator = Union[Literal["$in"], Literal["$nin"]]
OperatorExpression = Union[
    Dict[Union[WhereOperator, LogicalOperator], LiteralValue],
    Dict[InclusionExclusionOperator, List[LiteralValue]],
]

Where = Dict[
    Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, List["Where"]]
]

WhereDocumentOperator = Union[
    Literal["$contains"],
    Literal["$not_contains"],
    Literal["$regex"],
    Literal["$not_regex"],
    LogicalOperator,
]
WhereDocument = Dict[WhereDocumentOperator, Union[str, List["WhereDocument"]]]
