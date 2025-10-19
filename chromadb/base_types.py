from typing import Dict, List, Mapping, Optional, Sequence, Union, Any
from typing_extensions import Literal, Final
from dataclasses import dataclass
import numpy as np
from numpy.typing import NDArray

# Type tag constants
TYPE_KEY: Final[str] = "#type"
SPARSE_VECTOR_TYPE_VALUE: Final[str] = "sparse_vector"


@dataclass
class SparseVector:
    """Represents a sparse vector using parallel arrays for indices and values.

    Attributes:
        indices: List of dimension indices (must be non-negative integers, sorted in strictly ascending order)
        values: List of values corresponding to each index (floats)

    Note:
        - Indices must be sorted in strictly ascending order (no duplicates)
        - Indices and values must have the same length
        - All validations are performed in __post_init__
    """

    indices: List[int]
    values: List[float]

    def __post_init__(self) -> None:
        """Validate the sparse vector structure."""
        if not isinstance(self.indices, list):
            raise ValueError(
                f"Expected SparseVector indices to be a list, got {type(self.indices).__name__}"
            )

        if not isinstance(self.values, list):
            raise ValueError(
                f"Expected SparseVector values to be a list, got {type(self.values).__name__}"
            )

        if len(self.indices) != len(self.values):
            raise ValueError(
                f"SparseVector indices and values must have the same length, "
                f"got {len(self.indices)} indices and {len(self.values)} values"
            )

        for i, idx in enumerate(self.indices):
            if not isinstance(idx, int):
                raise ValueError(
                    f"SparseVector indices must be integers, got {type(idx).__name__} at position {i}"
                )
            if idx < 0:
                raise ValueError(
                    f"SparseVector indices must be non-negative, got {idx} at position {i}"
                )

        for i, val in enumerate(self.values):
            if not isinstance(val, (int, float)):
                raise ValueError(
                    f"SparseVector values must be numbers, got {type(val).__name__} at position {i}"
                )

        # Validate indices are sorted in strictly ascending order
        if len(self.indices) > 1:
            for i in range(1, len(self.indices)):
                if self.indices[i] <= self.indices[i - 1]:
                    raise ValueError(
                        f"SparseVector indices must be sorted in strictly ascending order, "
                        f"found indices[{i}]={self.indices[i]} <= indices[{i-1}]={self.indices[i-1]}"
                    )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to transport format with type tag."""
        return {
            TYPE_KEY: SPARSE_VECTOR_TYPE_VALUE,
            "indices": self.indices,
            "values": self.values,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "SparseVector":
        """Deserialize from transport format (strict - requires #type field)."""
        if d.get(TYPE_KEY) != SPARSE_VECTOR_TYPE_VALUE:
            raise ValueError(
                f"Expected {TYPE_KEY}='{SPARSE_VECTOR_TYPE_VALUE}', got {d.get(TYPE_KEY)}"
            )
        return cls(indices=d["indices"], values=d["values"])


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
