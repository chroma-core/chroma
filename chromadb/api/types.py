from typing import Optional, Union, Sequence, TypeVar, List, Dict, Any, Tuple
from typing_extensions import Literal, TypedDict, Protocol
import chromadb.errors as errors
from chromadb.types import (
    Metadata,
    UpdateMetadata,
    Vector,
    LiteralValue,
    LogicalOperator,
    WhereOperator,
    OperatorExpression,
    Where,
    WhereDocumentOperator,
    WhereDocument,
)

# Re-export types from chromadb.types
__all__ = ["Metadata", "Where", "WhereDocument", "UpdateCollectionMetadata"]

ID = str
IDs = List[ID]

Embedding = Vector
Embeddings = List[Embedding]

Metadatas = List[Metadata]

CollectionMetadata = Dict[str, Any]
UpdateCollectionMetadata = UpdateMetadata

Document = str
Documents = List[Document]

Parameter = TypeVar("Parameter", Embedding, Document, Metadata, ID)
T = TypeVar("T")
OneOrMany = Union[T, List[T]]

# This should ust be List[Literal["documents", "embeddings", "metadatas", "distances"]]
# However, this provokes an incompatibility with the Overrides library and Python 3.7
Include = List[
    Union[
        Literal["documents"],
        Literal["embeddings"],
        Literal["metadatas"],
        Literal["distances"],
    ]
]

# Re-export types from chromadb.types
LiteralValue = LiteralValue
LogicalOperator = LogicalOperator
WhereOperator = WhereOperator
OperatorExpression = OperatorExpression
Where = Where
WhereDocumentOperator = WhereDocumentOperator


class GetResult(TypedDict):
    ids: List[ID]
    embeddings: Optional[List[Embedding]]
    documents: Optional[List[Document]]
    metadatas: Optional[List[Metadata]]


class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: Optional[List[List[Embedding]]]
    documents: Optional[List[List[Document]]]
    metadatas: Optional[List[List[Metadata]]]
    distances: Optional[List[List[float]]]


class IndexMetadata(TypedDict):
    dimensionality: int
    # The current number of elements in the index (total = additions - deletes)
    curr_elements: int
    # The auto-incrementing ID of the last inserted element, never decreases so
    # can be used as a count of total historical size. Should increase by 1 every add.
    # Assume cannot overflow
    total_elements_added: int
    time_created: float


class EmbeddingFunction(Protocol):
    def __call__(self, texts: Documents) -> Embeddings:
        ...


def maybe_cast_one_to_many(
    target: OneOrMany[Parameter],
) -> List[Parameter]:
    """Infers if target is Embedding, Metadata, or Document and casts it to a many object if its one"""

    if isinstance(target, Sequence):
        # One Document or ID
        if isinstance(target, str) and target is not None:
            return [target]
        # One Embedding
        if isinstance(target[0], (int, float)):
            return [target]  # type: ignore
    # One Metadata dict
    if isinstance(target, dict):
        return [target]
    # Already a sequence
    return target  # type: ignore


def validate_ids(ids: IDs) -> IDs:
    """Validates ids to ensure it is a list of strings"""
    if not isinstance(ids, list):
        raise ValueError(f"Expected IDs to be a list, got {ids}")
    if len(ids) == 0:
        raise ValueError(f"Expected IDs to be a non-empty list, got {ids}")
    seen = set()
    dups = set()
    for id_ in ids:
        if not isinstance(id_, str):
            raise ValueError(f"Expected ID to be a str, got {id_}")
        if id_ in seen:
            dups.add(id_)
        else:
            seen.add(id_)
    if dups:
        n_dups = len(dups)
        if n_dups < 10:
            example_string = ", ".join(dups)
            message = (
                f"Expected IDs to be unique, found duplicates of: {example_string}"
            )
        else:
            examples = []
            for idx, dup in enumerate(dups):
                examples.append(dup)
                if idx == 10:
                    break
            example_string = (
                f"{', '.join(examples[:5])}, ..., {', '.join(examples[-5:])}"
            )
            message = f"Expected IDs to be unique, found {n_dups} duplicated IDs: {example_string}"
        raise errors.DuplicateIDError(message)
    return ids


def validate_metadata(metadata: Metadata) -> Metadata:
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, floats or bools"""
    if not isinstance(metadata, dict) and metadata is not None:
        raise ValueError(f"Expected metadata to be a dict or None, got {metadata}")
    if metadata is None:
        return metadata
    if len(metadata) == 0:
        raise ValueError(f"Expected metadata to be a non-empty dict, got {metadata}")
    for key, value in metadata.items():
        if not isinstance(key, str):
            raise ValueError(
                f"Expected metadata key to be a str, got {key} which is a {type(key)}"
            )
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        if not isinstance(value, bool) and not isinstance(value, (str, int, float)):
            raise ValueError(
                f"Expected metadata value to be a str, int, float or bool, got {value} which is a {type(value)}"
            )
    return metadata


def validate_update_metadata(metadata: UpdateMetadata) -> UpdateMetadata:
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, floats or bools"""
    if not isinstance(metadata, dict) and metadata is not None:
        raise ValueError(f"Expected metadata to be a dict or None, got {metadata}")
    if metadata is None:
        return metadata
    if len(metadata) == 0:
        raise ValueError(f"Expected metadata to be a non-empty dict, got {metadata}")
    for key, value in metadata.items():
        if not isinstance(key, str):
            raise ValueError(f"Expected metadata key to be a str, got {key}")
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        if not isinstance(value, bool) and not isinstance(
            value, (str, int, float, type(None))
        ):
            raise ValueError(
                f"Expected metadata value to be a str, int, or float, got {value}"
            )
    return metadata


def validate_metadatas(metadatas: Metadatas) -> Metadatas:
    """Validates metadatas to ensure it is a list of dictionaries of strings to strings, ints, floats or bools"""
    if not isinstance(metadatas, list):
        raise ValueError(f"Expected metadatas to be a list, got {metadatas}")
    for metadata in metadatas:
        validate_metadata(metadata)
    return metadatas


def validate_where(where: Where) -> Where:
    """
    Validates where to ensure it is a dictionary of strings to strings, ints, floats or operator expressions,
    or in the case of $and and $or, a list of where expressions
    """
    if not isinstance(where, dict):
        raise ValueError(f"Expected where to be a dict, got {where}")
    if len(where) != 1:
        raise ValueError(f"Expected where to have exactly one operator, got {where}")
    for key, value in where.items():
        if not isinstance(key, str):
            raise ValueError(f"Expected where key to be a str, got {key}")
        if (
            key != "$and"
            and key != "$or"
            and key != "$in"
            and key != "$nin"
            and not isinstance(value, (str, int, float, dict))
        ):
            raise ValueError(
                f"Expected where value to be a str, int, float, or operator expression, got {value}"
            )
        if key == "$and" or key == "$or":
            if not isinstance(value, list):
                raise ValueError(
                    f"Expected where value for $and or $or to be a list of where expressions, got {value}"
                )
            if len(value) <= 1:
                raise ValueError(
                    f"Expected where value for $and or $or to be a list with at least two where expressions, got {value}"
                )
            for where_expression in value:
                validate_where(where_expression)
        # Value is a operator expression
        if isinstance(value, dict):
            # Ensure there is only one operator
            if len(value) != 1:
                raise ValueError(
                    f"Expected operator expression to have exactly one operator, got {value}"
                )

            for operator, operand in value.items():
                # Only numbers can be compared with gt, gte, lt, lte
                if operator in ["$gt", "$gte", "$lt", "$lte"]:
                    if not isinstance(operand, (int, float)):
                        raise ValueError(
                            f"Expected operand value to be an int or a float for operator {operator}, got {operand}"
                        )
                if operator in ["$in", "$nin"]:
                    if not isinstance(operand, list):
                        raise ValueError(
                            f"Expected operand value to be an list for operator {operator}, got {operand}"
                        )
                if operator not in [
                    "$gt",
                    "$gte",
                    "$lt",
                    "$lte",
                    "$ne",
                    "$eq",
                    "$in",
                    "$nin",
                ]:
                    raise ValueError(
                        f"Expected where operator to be one of $gt, $gte, $lt, $lte, $ne, $eq, $in, $nin, "
                        f"got {operator}"
                    )

                if not isinstance(operand, (str, int, float, list)):
                    raise ValueError(
                        f"Expected where operand value to be a str, int, float, or list of those type, got {operand}"
                    )
                if isinstance(operand, list) and (
                    len(operand) == 0
                    or not all(isinstance(x, type(operand[0])) for x in operand)
                ):
                    raise ValueError(
                        f"Expected where operand value to be a non-empty list, and all values to obe of the same type "
                        f"got {operand}"
                    )
    return where


def validate_where_document(where_document: WhereDocument) -> WhereDocument:
    """
    Validates where_document to ensure it is a dictionary of WhereDocumentOperator to strings, or in the case of $and and $or,
    a list of where_document expressions
    """
    if not isinstance(where_document, dict):
        raise ValueError(
            f"Expected where document to be a dictionary, got {where_document}"
        )
    if len(where_document) != 1:
        raise ValueError(
            f"Expected where document to have exactly one operator, got {where_document}"
        )
    for operator, operand in where_document.items():
        if operator not in ["$contains", "$and", "$or"]:
            raise ValueError(
                f"Expected where document operator to be one of $contains, $and, $or, got {operator}"
            )
        if operator == "$and" or operator == "$or":
            if not isinstance(operand, list):
                raise ValueError(
                    f"Expected document value for $and or $or to be a list of where document expressions, got {operand}"
                )
            if len(operand) <= 1:
                raise ValueError(
                    f"Expected document value for $and or $or to be a list with at least two where document expressions, got {operand}"
                )
            for where_document_expression in operand:
                validate_where_document(where_document_expression)
        # Value is a $contains operator
        elif not isinstance(operand, str):
            raise ValueError(
                f"Expected where document operand value for operator $contains to be a str, got {operand}"
            )
        elif len(operand) == 0:
            raise ValueError(
                "Expected where document operand value for operator $contains to be a non-empty str"
            )
    return where_document


def validate_include(include: Include, allow_distances: bool) -> Include:
    """Validates include to ensure it is a list of strings. Since get does not allow distances, allow_distances is used
    to control if distances is allowed"""

    if not isinstance(include, list):
        raise ValueError(f"Expected include to be a list, got {include}")
    for item in include:
        if not isinstance(item, str):
            raise ValueError(f"Expected include item to be a str, got {item}")
        allowed_values = ["embeddings", "documents", "metadatas"]
        if allow_distances:
            allowed_values.append("distances")
        if item not in allowed_values:
            raise ValueError(
                f"Expected include item to be one of {', '.join(allowed_values)}, got {item}"
            )
    return include


def validate_n_results(n_results: int) -> int:
    """Validates n_results to ensure it is a positive Integer. Since hnswlib does not allow n_results to be negative."""
    # Check Number of requested results
    if not isinstance(n_results, int):
        raise ValueError(
            f"Expected requested number of results to be a int, got {n_results}"
        )
    if n_results <= 0:
        raise TypeError(
            f"Number of requested results {n_results}, cannot be negative, or zero."
        )
    return n_results


def validate_embeddings(embeddings: Embeddings) -> Embeddings:
    """Validates embeddings to ensure it is a list of list of ints, or floats"""
    if not isinstance(embeddings, list):
        raise ValueError(f"Expected embeddings to be a list, got {embeddings}")
    if len(embeddings) == 0:
        raise ValueError(
            f"Expected embeddings to be a list with at least one item, got {embeddings}"
        )
    if not all([isinstance(e, list) for e in embeddings]):
        raise ValueError(
            f"Expected each embedding in the embeddings to be a list, got {embeddings}"
        )
    for embedding in embeddings:
        if not all([isinstance(value, (int, float)) for value in embedding]):
            raise ValueError(
                f"Expected each value in the embedding to be a int or float, got {embeddings}"
            )
    return embeddings


def validate_batch(
    batch: Tuple[IDs, Optional[Embeddings], Optional[Metadatas], Optional[Documents]],
    limits: Dict[str, Any],
) -> None:
    if len(batch[0]) > limits["max_batch_size"]:
        raise ValueError(
            f"Batch size {len(batch[0])} exceeds maximum batch size {limits['max_batch_size']}"
        )
