from typing import Optional, Union, TypeVar, List, Dict, Any, Tuple, cast
from numpy.typing import NDArray
import numpy as np
from typing_extensions import TypedDict, Protocol, runtime_checkable
from enum import Enum
from pydantic import Field
import chromadb.errors as errors
from chromadb.types import (
    Metadata,
    UpdateMetadata,
    Vector,
    PyVector,
    LiteralValue,
    LogicalOperator,
    WhereOperator,
    OperatorExpression,
    Where,
    WhereDocumentOperator,
    WhereDocument,
)
from inspect import signature
from tenacity import retry

# Re-export types from chromadb.types
__all__ = ["Metadata", "Where", "WhereDocument", "UpdateCollectionMetadata"]
META_KEY_CHROMA_DOCUMENT = "chroma:document"
T = TypeVar("T")
OneOrMany = Union[T, List[T]]

# URIs
URI = str
URIs = List[URI]


def maybe_cast_one_to_many(target: Optional[(OneOrMany[T])]) -> Optional[List[T]]:
    # No target
    if target is None:
        return None

    if isinstance(target, str) or isinstance(target, dict) or is_image(target):
        # One URI
        return cast(List[T], [target])

    return cast(List[T], target)


# IDs
ID = str
IDs = List[ID]


# Embeddings
PyEmbedding = PyVector
PyEmbeddings = List[PyEmbedding]
Embedding = Vector
Embeddings = List[Embedding]


def maybe_cast_one_to_many_embedding(
    target: Union[Optional[OneOrMany[Embedding]], Optional[OneOrMany[PyEmbedding]]]
) -> Optional[Embeddings]:
    if target is None:
        return None

    if not isinstance(target, list) and not isinstance(target, np.ndarray):
        raise ValueError(
            f"Expected embeddings to be a list or a numpy array, got {type(target).__name__}"
        )

    if len(target) == 0:
        raise ValueError(
            "Expected embeddings to be a list or a numpy array with at least one item"
        )

    if isinstance(target, np.ndarray):
        dim = target.ndim
        if dim == 1:
            # TODO: Remove this conversion when unpacking
            return cast(Embeddings, [target.tolist()])
        if dim == 2:
            return cast(Embeddings, target.tolist())
        raise ValueError(
            f"Expected embeddings to be a 1D or 2D numpy array, got {dim}D"
        )

    if isinstance(target, list):
        # target represents a single embedding as a list
        if isinstance(target[0], (int, float)):
            return cast(Embeddings, [target])

        # Check if the first item is a numpy array - target is a list of numpy arrays
        if isinstance(target[0], np.ndarray):
            # Check all the embeddings are 1D
            for embedding in target:
                dim = (cast(np.ndarray, embedding)).ndim  # type: ignore[type-arg]
                if dim != 1:
                    raise ValueError(
                        f"Expected embeddings to be a list of 1D numpy arrays, got a {dim}D numpy array"
                    )
            return [cast(np.ndarray, embedding).tolist() for embedding in target]  # type: ignore[type-arg]

    # target is a list of lists representing embeddings
    return cast(Embeddings, target)


# Metadatas
Metadatas = List[Metadata]


CollectionMetadata = Dict[str, Any]
UpdateCollectionMetadata = UpdateMetadata

# Documents
Document = str
Documents = List[Document]


def is_document(target: Any) -> bool:
    if not isinstance(target, str):
        return False
    return True


# Images
ImageDType = Union[np.uint, np.int64, np.float64]
Image = NDArray[ImageDType]
Images = List[Image]


def is_image(target: Any) -> bool:
    if not isinstance(target, np.ndarray):
        return False
    if len(target.shape) < 2:
        return False
    return True


Parameter = TypeVar("Parameter", Document, Image, Embedding, Metadata, ID)


class IncludeEnum(str, Enum):
    documents = "documents"
    embeddings = "embeddings"
    metadatas = "metadatas"
    distances = "distances"
    uris = "uris"
    data = "data"


# This should ust be List[Literal["documents", "embeddings", "metadatas", "distances"]]
# However, this provokes an incompatibility with the Overrides library and Python 3.7
Include = List[IncludeEnum]
IncludeMetadataDocuments = Field(default=["metadatas", "documents"])
IncludeMetadataDocumentsEmbeddings = Field(
    default=["metadatas", "documents", "embeddings"]
)
IncludeMetadataDocumentsEmbeddingsDistances = Field(
    default=["metadatas", "documents", "embeddings", "distances"]
)
IncludeMetadataDocumentsDistances = Field(
    default=["metadatas", "documents", "distances"]
)


class RecordSet(TypedDict):
    ids: Optional[IDs]
    embeddings: Optional[Embeddings]
    metadatas: Optional[Metadatas]
    documents: Optional[Documents]
    images: Optional[Images]
    uris: Optional[URIs]


def record_set_contains_one_of(record_set: RecordSet, include: Include) -> bool:
    """Check if the record set contains data for any of the given include keys"""
    if len(include) == 0:
        raise ValueError("Expected include to be a non-empty list")

    error_messages = []
    for include_key in include:
        if include_key not in record_set:
            error_messages.append(
                f"Expected include key to be a a known field of RecordSet, got {include_key}"
            )

    if len(error_messages) > 0:
        raise ValueError(", ".join(error_messages))

    for record_key, value in record_set.items():
        if record_key not in include:
            continue

        if isinstance(value, list):
            if len(value) == 0:
                raise ValueError(f"Expected {record_key} to be a non-empty list")

            return True

    return False


# Re-export types from chromadb.types
LiteralValue = LiteralValue
LogicalOperator = LogicalOperator
WhereOperator = WhereOperator
OperatorExpression = OperatorExpression
Where = Where
WhereDocumentOperator = WhereDocumentOperator

Embeddable = Union[Documents, Images]
D = TypeVar("D", bound=Embeddable, contravariant=True)


Loadable = List[Optional[Image]]
L = TypeVar("L", covariant=True, bound=Loadable)


class GetResult(TypedDict):
    ids: List[ID]
    embeddings: Optional[
        Union[Embeddings, PyEmbeddings, NDArray[Union[np.int32, np.float32]]]
    ]
    documents: Optional[List[Document]]
    uris: Optional[URIs]
    data: Optional[Loadable]
    metadatas: Optional[List[Metadata]]
    included: Include


class AddResult(TypedDict):
    ids: List[ID]


class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: Optional[
        Union[
            List[Embeddings],
            List[PyEmbeddings],
            List[NDArray[Union[np.int32, np.float32]]],
        ]
    ]
    documents: Optional[List[List[Document]]]
    uris: Optional[List[List[URI]]]
    data: Optional[List[Loadable]]
    metadatas: Optional[List[List[Metadata]]]
    distances: Optional[List[List[float]]]
    included: Include


class IndexMetadata(TypedDict):
    dimensionality: int
    # The current number of elements in the index (total = additions - deletes)
    curr_elements: int
    # The auto-incrementing ID of the last inserted element, never decreases so
    # can be used as a count of total historical size. Should increase by 1 every add.
    # Assume cannot overflow
    total_elements_added: int
    time_created: float


@runtime_checkable
class EmbeddingFunction(Protocol[D]):
    def __call__(self, input: D) -> Embeddings:
        ...

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: EmbeddingFunction[D], input: D) -> Embeddings:
            result = call(self, input)

            return validate_embeddings(result)

        setattr(cls, "__call__", __call__)

    def embed_with_retries(
        self, input: D, **retry_kwargs: Dict[str, Any]
    ) -> Embeddings:
        return cast(Embeddings, retry(**retry_kwargs)(self.__call__)(input))


def normalize_embeddings(
    embeddings: Union[
        OneOrMany[Embedding],
        OneOrMany[PyEmbedding],
    ]
) -> Embeddings:
    return cast(Embeddings, [np.array(embedding) for embedding in embeddings])


def validate_embedding_function(
    embedding_function: EmbeddingFunction[Embeddable],
) -> None:
    function_signature = signature(
        embedding_function.__class__.__call__
    ).parameters.keys()
    protocol_signature = signature(EmbeddingFunction.__call__).parameters.keys()

    if not function_signature == protocol_signature:
        raise ValueError(
            f"Expected EmbeddingFunction.__call__ to have the following signature: {protocol_signature}, got {function_signature}\n"
            "Please see https://docs.trychroma.com/guides/embeddings for details of the EmbeddingFunction interface.\n"
            "Please note the recent change to the EmbeddingFunction interface: https://docs.trychroma.com/deployment/migration#migration-to-0.4.16---november-7,-2023 \n"
        )


class DataLoader(Protocol[L]):
    def __call__(self, uris: URIs) -> L:
        ...


def validate_ids(ids: IDs) -> IDs:
    """Validates ids to ensure it is a list of strings"""
    if not isinstance(ids, list):
        raise ValueError(f"Expected IDs to be a list, got {type(ids).__name__} as IDs")
    if len(ids) == 0:
        raise ValueError(f"Expected IDs to be a non-empty list, got {len(ids)} IDs")

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
        raise ValueError(
            f"Expected metadata to be a dict or None, got {type(metadata).__name__} as metadata"
        )
    if metadata is None:
        return metadata
    if len(metadata) == 0:
        raise ValueError(
            f"Expected metadata to be a non-empty dict, got {len(metadata)} metadata attributes"
        )
    for key, value in metadata.items():
        if key == META_KEY_CHROMA_DOCUMENT:
            raise ValueError(
                f"Expected metadata to not contain the reserved key {META_KEY_CHROMA_DOCUMENT}"
            )
        if not isinstance(key, str):
            raise TypeError(
                f"Expected metadata key to be a str, got {key} which is a {type(key).__name__}"
            )
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        if not isinstance(value, bool) and not isinstance(value, (str, int, float)):
            raise ValueError(
                f"Expected metadata value to be a str, int, float or bool, got {value} which is a {type(value).__name__}"
            )
    return metadata


def validate_update_metadata(metadata: UpdateMetadata) -> UpdateMetadata:
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, floats or bools"""
    if not isinstance(metadata, dict) and metadata is not None:
        raise ValueError(
            f"Expected metadata to be a dict or None, got {type(metadata)}"
        )
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
                        f"Expected where operand value to be a non-empty list, and all values to be of the same type "
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
        if operator not in ["$contains", "$not_contains", "$and", "$or"]:
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
        allowed_values = ["embeddings", "documents", "metadatas", "uris", "data"]
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
    """Validates embeddings to ensure it is a list of numpy arrays of ints, or floats"""
    if not isinstance(embeddings, (list, np.ndarray)):
        raise ValueError(
            f"Expected embeddings to be a list, got {type(embeddings).__name__}"
        )
    if len(embeddings) == 0:
        raise ValueError(
            f"Expected embeddings to be a list with at least one item, got {len(embeddings)} embeddings"
        )
    if not all([isinstance(e, np.ndarray) for e in embeddings]):
        raise ValueError(
            "Expected each embedding in the embeddings to be a numpy array, got "
            f"{list(set([type(e).__name__ for e in embeddings]))}"
        )
    for i, embedding in enumerate(embeddings):
        if embedding.ndim == 0:
            raise ValueError(
                f"Expected a 1-dimensional array, got a 0-dimensional array {embedding}"
            )
        if embedding.size == 0:
            raise ValueError(
                f"Expected each embedding in the embeddings to be a 1-dimensional numpy array with at least 1 int/float value. Got a 1-dimensional numpy array with no values at pos {i}"
            )
        if not all(
            [
                isinstance(value, (np.integer, float, np.floating))
                and not isinstance(value, bool)
                for value in embedding
            ]
        ):
            raise ValueError(
                "Expected each value in the embedding to be a int or float, got an embedding with "
                f"{list(set([type(value).__name__ for value in embedding]))} - {embedding}"
            )
    return embeddings


def validate_batch_size(
    record_set: RecordSet,
    limits: Dict[str, Any],
) -> None:
    (_, batch_size) = get_n_items_from_record_set(record_set)

    if batch_size > limits["max_batch_size"]:
        raise ValueError(
            f"Batch size {batch_size} exceeds maximum batch size {limits['max_batch_size']}"
        )


def validate_record_set_consistency(record_set: RecordSet) -> None:
    """
    Validate the consistency of the record set, ensuring all values are non-empty lists and have the same length.
    """
    error_messages = []
    field_record_counts = []
    count = 0
    consistentcy_error = False

    for field, value in record_set.items():
        if value is None:
            continue

        if not isinstance(value, list):
            error_messages.append(
                f"Expected field {field} to be a list, got {type(value).__name__}"
            )
            continue

        if len(value) == 0:
            error_messages.append(
                f"Expected field {field} to be a non-empty list, got an empty list"
            )
            continue

        n_items = len(value)
        field_record_counts.append(f"{field}: ({n_items})")
        if count == 0:
            count = n_items
        elif count != n_items:
            consistentcy_error = True

    if consistentcy_error:
        error_messages.append(
            f"Inconsistent number of records: {', '.join(field_record_counts)}"
        )

    if len(error_messages) > 0:
        raise ValueError(", ".join(error_messages))


def validate_record_set(
    record_set: RecordSet,
    require_data: bool,
) -> None:
    validate_ids(record_set["ids"])
    validate_embeddings(record_set["embeddings"]) if record_set[
        "embeddings"
    ] is not None else None
    validate_metadatas(record_set["metadatas"]) if record_set[
        "metadatas"
    ] is not None else None

    # Only one of documents or images can be provided
    if record_set["documents"] is not None and record_set["images"] is not None:
        raise ValueError("You can only provide documents or images, not both.")

    required_fields: Include = ["embeddings", "documents", "images", "uris"]  # type: ignore[list-item]
    if not require_data:
        required_fields += ["metadatas"]  # type: ignore[list-item]

    if not record_set_contains_one_of(record_set, include=required_fields):
        raise ValueError(f"You must provide one of {', '.join(required_fields)}")

    valid_ids = record_set["ids"]
    for key in ["embeddings", "metadatas", "documents", "images", "uris"]:
        if record_set[key] is not None and len(record_set[key]) != len(valid_ids):  # type: ignore[literal-required]
            raise ValueError(
                f"Number of {key} {len(record_set[key])} must match number of ids {len(valid_ids)}"  # type: ignore[literal-required]
            )


def get_n_items_from_record_set(
    record_set: RecordSet, should_validate: bool = True
) -> Tuple[str, int]:
    """
    Get the number of items in the record set.
    """
    if should_validate:
        validate_record_set_consistency(record_set)

    for field, value in record_set.items():
        if isinstance(value, list) and len(value) > 0:
            return field, len(value)

    return "", 0


def convert_np_embeddings_to_list(embeddings: Embeddings) -> PyEmbeddings:
    return [embedding.tolist() for embedding in embeddings]


def convert_list_embeddings_to_np(embeddings: PyEmbeddings) -> Embeddings:
    return [np.array(embedding) for embedding in embeddings]
