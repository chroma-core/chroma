from typing import (
    Optional,
    Set,
    Union,
    TypeVar,
    List,
    Dict,
    Any,
    Tuple,
    cast,
    Literal,
    get_args,
    TYPE_CHECKING,
    Final,
    Type,
)
from copy import deepcopy
from typing_extensions import TypeAlias
from dataclasses import dataclass
from numpy.typing import NDArray
import numpy as np
import warnings
from typing_extensions import TypedDict, Protocol, runtime_checkable
from pydantic import BaseModel, field_validator, model_validator
from pydantic_core import PydanticCustomError

import chromadb.errors as errors
from chromadb.base_types import (
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
    SparseVector,
)

if TYPE_CHECKING:
    from chromadb.execution.expression.operator import Key

try:
    from chromadb.is_thin_client import is_thin_client
except ImportError:
    is_thin_client = False
from inspect import signature
from tenacity import retry
from abc import abstractmethod
import pybase64
from functools import lru_cache
import struct
import math

# Re-export types from chromadb.types
__all__ = [
    "Metadata",
    "Where",
    "WhereDocument",
    "UpdateCollectionMetadata",
    "UpdateMetadata",
    "SearchResult",
    "SearchResultRow",
    "SparseVector",
    # Index Configuration Types
    "FtsIndexConfig",
    "HnswIndexConfig",
    "SpannIndexConfig",
    "VectorIndexConfig",
    "SparseVectorIndexConfig",
    "StringInvertedIndexConfig",
    "IntInvertedIndexConfig",
    "FloatInvertedIndexConfig",
    "BoolInvertedIndexConfig",
    "IndexConfig",
    # New Schema System (mirrors Rust Schema)
    "Schema",
    "ValueTypes",
    "StringValueType",
    "FloatListValueType",
    "SparseVectorValueType",
    "IntValueType",
    "FloatValueType",
    "BoolValueType",
    # Index Type Classes
    "FtsIndexType",
    "VectorIndexType",
    "SparseVectorIndexType",
    "StringInvertedIndexType",
    "IntInvertedIndexType",
    "FloatInvertedIndexType",
    "BoolInvertedIndexType",
    # Value Type Constants
    "STRING_VALUE_NAME",
    "INT_VALUE_NAME",
    "BOOL_VALUE_NAME",
    "FLOAT_VALUE_NAME",
    "FLOAT_LIST_VALUE_NAME",
    "SPARSE_VECTOR_VALUE_NAME",
    # Index Name Constants
    "FTS_INDEX_NAME",
    "VECTOR_INDEX_NAME",
    "SPARSE_VECTOR_INDEX_NAME",
    "STRING_INVERTED_INDEX_NAME",
    "INT_INVERTED_INDEX_NAME",
    "FLOAT_INVERTED_INDEX_NAME",
    "BOOL_INVERTED_INDEX_NAME",
    "HNSW_INDEX_NAME",
    "SPANN_INDEX_NAME",
    "DOCUMENT_KEY",
    "EMBEDDING_KEY",
    "TYPE_KEY",
    "SPARSE_VECTOR_TYPE_VALUE",
    # Space type
    "Space",
    # Embedding Functions
    "EmbeddingFunction",
    "SparseEmbeddingFunction",
    "validate_embedding_function",
    "validate_sparse_embedding_function",
    # Sparse vectors
    "SparseVector",
    "SparseVectors",
    "validate_sparse_vectors",
]
META_KEY_CHROMA_DOCUMENT = "chroma:document"
T = TypeVar("T")
OneOrMany = Union[T, List[T]]


def maybe_cast_one_to_many(target: Optional[OneOrMany[T]]) -> Optional[List[T]]:
    if target is None:
        return None
    if isinstance(target, list):
        return target
    return [target]


# URIs
URI = str
URIs = List[URI]

# IDs
ID = str
IDs = List[ID]

# Embeddings
PyEmbedding = PyVector
PyEmbeddings = List[PyEmbedding]
Embedding = Vector
Embeddings = List[Embedding]
SparseVectors = List[SparseVector]


@lru_cache
def _get_struct(vector_length: int) -> struct.Struct:
    return struct.Struct(f"<{vector_length}f")


def _to_f32(value: float) -> float:
    F32_MAX = np.finfo(np.float32).max
    F32_MIN = np.finfo(np.float32).min
    if math.isnan(value):
        return float("nan")
    if value > F32_MAX:
        return float("inf")
    if value < F32_MIN:
        return float("-inf")
    return value


def pack_embedding_safely(embedding: Embedding) -> str:
    try:
        return pybase64.b64encode_as_string(
            _get_struct(len(embedding)).pack(*embedding)
        )
    except OverflowError:
        return pybase64.b64encode_as_string(
            _get_struct(len(embedding)).pack(*[_to_f32(value) for value in embedding])
        )


# returns base64 encoded embeddings or None if the embedding is None
# currently, PyEmbeddings can't have None, but this is to future proof, we want to be able to handle None embeddings
def optional_embeddings_to_base64_strings(
    embeddings: Optional[Embeddings],
) -> Optional[list[Union[str, None]]]:
    if embeddings is None:
        return None
    return [
        pack_embedding_safely(embedding) if embedding is not None else None
        for embedding in embeddings
    ]


def optional_base64_strings_to_embeddings(
    b64_strings: Optional[list[Union[str, None]]],
) -> Optional[PyEmbeddings]:
    if b64_strings is None:
        return None

    embeddings: PyEmbeddings = []
    for b64_string in b64_strings:
        if b64_string is None:
            embeddings.append(None)  # type: ignore
        else:
            packed_data = pybase64.b64decode(b64_string)
            vector_length = len(packed_data) // 4
            embedding_tuple = _get_struct(vector_length).unpack(packed_data)
            embeddings.append(list(embedding_tuple))
    return embeddings


def normalize_embeddings(
    target: Optional[Union[OneOrMany[Embedding], OneOrMany[PyEmbedding]]]
) -> Optional[Embeddings]:
    if target is None:
        return None

    if len(target) == 0:
        raise ValueError(
            f"Expected Embeddings to be non-empty list or numpy array, got {target}"
        )

    if isinstance(target, np.ndarray):
        if target.ndim == 1:
            return [target]
        elif target.ndim == 2:
            return [row for row in target]
    elif isinstance(target, list):
        # One PyEmbedding
        if isinstance(target[0], (int, float)) and not isinstance(target[0], bool):
            return [np.array(target, dtype=np.float32)]
        elif isinstance(target[0], np.ndarray):
            return cast(Embeddings, target)
        elif isinstance(target[0], list):
            if isinstance(target[0][0], (int, float)) and not isinstance(
                target[0][0], bool
            ):
                return [np.array(row, dtype=np.float32) for row in target]

    raise ValueError(
        f"Expected embeddings to be a list of floats or ints, a list of lists, a numpy array, or a list of numpy arrays, got {target}"
    )


# Metadatas
Metadatas = List[Metadata]

CollectionMetadata = Dict[str, Any]
UpdateCollectionMetadata = UpdateMetadata


def normalize_metadata(metadata: Optional[Metadata]) -> Optional[Metadata]:
    """
    Normalize metadata by converting dict-format sparse vectors to SparseVector instances.

    Accepts:
    - SparseVector instances (pass through)
    - Dict with #type='sparse_vector' (convert to SparseVector)
    - Other primitive types (pass through)

    Returns: Metadata with all sparse vectors as SparseVector instances

    Note: This allows users to provide sparse vectors in dict format for convenience.
    The dict format is automatically converted to SparseVector instances, which are
    then validated by SparseVector.__post_init__.
    """
    if metadata is None:
        return metadata

    normalized = {}
    for key, value in metadata.items():
        if isinstance(value, dict) and value.get(TYPE_KEY) == SPARSE_VECTOR_TYPE_VALUE:
            # Convert dict format to SparseVector (validates via __post_init__)
            normalized[key] = SparseVector.from_dict(value)
        else:
            # Pass through (including existing SparseVector instances)
            normalized[key] = value

    return normalized


def normalize_metadatas(
    metadatas: Optional[OneOrMany[Metadata]],
) -> Optional[List[Optional[Metadata]]]:
    """
    Normalize metadatas list, converting dict-format sparse vectors to instances.

    Handles both single metadata dict and list of metadata dicts.
    Converts any dict-format sparse vectors to SparseVector instances.

    Note: Individual items can be None (e.g., when embeddings are provided without metadata).
    """
    unpacked = maybe_cast_one_to_many(metadatas)
    if unpacked is None:
        return None

    return [normalize_metadata(m) for m in unpacked]


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


class BaseRecordSet(TypedDict):
    """
    The base record set includes 'data' fields which can be embedded, and embeddings.
    """

    embeddings: Optional[Embeddings]
    documents: Optional[Documents]
    images: Optional[Images]
    uris: Optional[URIs]


def get_default_embeddable_record_set_fields() -> Set[str]:
    """
    Returns the set of fields that can be embedded on a Record Set.
    This is a way to avoid hardcoding the fields in multiple places,
    and keeps them immutable.
    """
    return {"documents", "images", "uris"}


class InsertRecordSet(BaseRecordSet):
    """
    A set of records for inserting.
    """

    ids: IDs
    metadatas: Optional[Metadatas]


def normalize_base_record_set(
    embeddings: Optional[Union[OneOrMany[Embedding], OneOrMany[PyEmbedding]]] = None,
    documents: Optional[OneOrMany[Document]] = None,
    images: Optional[OneOrMany[Image]] = None,
    uris: Optional[OneOrMany[URI]] = None,
) -> BaseRecordSet:
    """
    Unpacks and normalizes the fields of a BaseRecordSet.
    """

    return BaseRecordSet(
        embeddings=normalize_embeddings(embeddings),
        documents=maybe_cast_one_to_many(documents),
        images=maybe_cast_one_to_many(images),
        uris=maybe_cast_one_to_many(uris),
    )


def normalize_insert_record_set(
    ids: OneOrMany[ID],
    embeddings: Optional[
        Union[
            OneOrMany[Embedding],
            OneOrMany[PyEmbedding],
        ]
    ],
    metadatas: Optional[OneOrMany[Metadata]] = None,
    documents: Optional[OneOrMany[Document]] = None,
    images: Optional[OneOrMany[Image]] = None,
    uris: Optional[OneOrMany[URI]] = None,
) -> InsertRecordSet:
    """
    Unpacks and normalizes the fields of an InsertRecordSet.

    Normalization includes:
    - Converting various embedding formats to List[np.ndarray]
    - Converting dict-format sparse vectors to SparseVector instances
    - Converting single values to lists where appropriate
    """
    base_record_set = normalize_base_record_set(
        embeddings=embeddings, documents=documents, images=images, uris=uris
    )

    return InsertRecordSet(
        ids=cast(IDs, maybe_cast_one_to_many(ids)),
        metadatas=normalize_metadatas(metadatas),  # type: ignore[typeddict-item]
        embeddings=base_record_set["embeddings"],
        documents=base_record_set["documents"],
        images=base_record_set["images"],
        uris=base_record_set["uris"],
    )


def validate_base_record_set(record_set: BaseRecordSet) -> None:
    """
    Validates the RecordSet, ensuring that all fields are of the right type and length.
    """
    _validate_record_set_length_consistency(record_set)

    if record_set["embeddings"] is not None:
        validate_embeddings(embeddings=record_set["embeddings"])
    if record_set["documents"] is not None:
        validate_documents(
            documents=record_set["documents"],
            # If embeddings are present, some documents can be None
            nullable=(record_set["embeddings"] is not None),
        )
    if record_set["images"] is not None:
        validate_images(images=record_set["images"])

    # TODO: Validate URIs


def validate_insert_record_set(record_set: InsertRecordSet) -> None:
    """
    Validates the InsertRecordSet, ensuring that all fields are of the right type and length.
    """
    _validate_record_set_length_consistency(record_set)
    validate_base_record_set(record_set)

    validate_ids(record_set["ids"])
    if record_set["metadatas"] is not None:
        validate_metadatas(record_set["metadatas"])


def _validate_record_set_length_consistency(record_set: BaseRecordSet) -> None:
    lengths = [len(lst) for lst in record_set.values() if lst is not None]  # type: ignore[arg-type]

    if not lengths:
        raise ValueError(
            f"At least one of one of {', '.join(record_set.keys())} must be provided"
        )

    zero_lengths = [
        key for key, lst in record_set.items() if lst is not None and len(lst) == 0  # type: ignore[arg-type]
    ]

    if zero_lengths:
        raise ValueError(f"Non-empty lists are required for {zero_lengths}")

    if len(set(lengths)) > 1:
        error_str = ", ".join(
            f"{key}: {len(lst)}" for key, lst in record_set.items() if lst is not None  # type: ignore[arg-type]
        )
        raise ValueError(f"Unequal lengths for fields: {error_str}")


def validate_record_set_for_embedding(
    record_set: BaseRecordSet, embeddable_fields: Optional[Set[str]] = None
) -> None:
    """
    Validates that the Record is ready to be embedded, i.e. that it contains exactly one of the embeddable fields.
    """
    if record_set["embeddings"] is not None:
        raise ValueError("Attempting to embed a record that already has embeddings.")
    if embeddable_fields is None:
        embeddable_fields = get_default_embeddable_record_set_fields()
    validate_record_set_contains_one(record_set, embeddable_fields)


def validate_record_set_contains_any(
    record_set: BaseRecordSet, contains_any: Set[str]
) -> None:
    """
    Validates that at least one of the fields in contains_any is not None.
    """
    _validate_record_set_contains(record_set, contains_any)

    if not any(record_set[field] is not None for field in contains_any):  # type: ignore[literal-required]
        raise ValueError(f"At least one of {', '.join(contains_any)} must be provided")


def validate_record_set_contains_one(
    record_set: BaseRecordSet, contains_one: Set[str]
) -> None:
    """
    Validates that exactly one of the fields in contains_one is not None.
    """
    _validate_record_set_contains(record_set, contains_one)
    if sum(record_set[field] is not None for field in contains_one) != 1:  # type: ignore[literal-required]
        raise ValueError(f"Exactly one of {', '.join(contains_one)} must be provided")


def _validate_record_set_contains(
    record_set: BaseRecordSet, contains: Set[str]
) -> None:
    """
    Validates that all fields in contains are valid fields of the Record.
    """
    if any(field not in record_set for field in contains):
        raise ValueError(
            f"Invalid field in contains: {', '.join(contains)}, available fields: {', '.join(record_set.keys())}"
        )


Parameter = TypeVar("Parameter", Document, Image, Embedding, Metadata, ID)

Include = List[
    Literal["documents", "embeddings", "metadatas", "distances", "uris", "data"]
]
IncludeMetadataDocuments: Include = ["metadatas", "documents"]
IncludeMetadataDocumentsEmbeddings: Include = ["metadatas", "documents", "embeddings"]
IncludeMetadataDocumentsEmbeddingsDistances: Include = [
    "metadatas",
    "documents",
    "embeddings",
    "distances",
]
IncludeMetadataDocumentsDistances: Include = ["metadatas", "documents", "distances"]

# Re-export types from chromadb.types
LiteralValue = LiteralValue
LogicalOperator = LogicalOperator
WhereOperator = WhereOperator
OperatorExpression = OperatorExpression
Where = Where
WhereDocumentOperator = WhereDocumentOperator


class FilterSet(TypedDict):
    where: Optional[Where]
    where_document: Optional[WhereDocument]


def validate_filter_set(filter_set: FilterSet) -> None:
    if filter_set["where"] is not None:
        validate_where(filter_set["where"])
    if filter_set["where_document"] is not None:
        validate_where_document(filter_set["where_document"])


Embeddable = Union[Documents, Images]
D = TypeVar("D", bound=Embeddable, contravariant=True)

Loadable = List[Optional[Image]]
L = TypeVar("L", covariant=True, bound=Loadable)


class AddRequest(TypedDict):
    ids: IDs
    embeddings: Embeddings
    metadatas: Optional[Metadatas]
    documents: Optional[Documents]
    uris: Optional[URIs]


# Add result doesn't exist.


class GetRequest(TypedDict):
    ids: Optional[IDs]
    where: Optional[Where]
    where_document: Optional[WhereDocument]
    include: Include


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


class QueryRequest(TypedDict):
    embeddings: Embeddings
    ids: Optional[IDs]
    where: Optional[Where]
    where_document: Optional[WhereDocument]
    include: Include
    n_results: int


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


class SearchResultRow(TypedDict, total=False):
    """A single row from search results.

    Only includes fields that were actually returned in the search.
    The 'id' field is always present.
    """

    id: str  # Always present
    document: Optional[str]
    embedding: Optional[List[float]]
    metadata: Optional[Dict[str, Any]]
    score: Optional[float]


class SearchResult(dict):  # type: ignore
    """Column-major response from the search API with conversion methods.

    Inherits from dict to maintain backward compatibility with existing code
    that treats SearchResult as a dictionary.

    Structure:
        - ids: List[List[str]] - Always present
        - documents: List[Optional[List[Optional[str]]]] - Optional per payload
        - embeddings: List[Optional[List[Optional[List[float]]]]] - Optional per payload
        - metadatas: List[Optional[List[Optional[Dict[str, Any]]]]] - Optional per payload
        - scores: List[Optional[List[Optional[float]]]] - Optional per payload
        - select: List[List[str]] - Selected fields for each payload
    """

    # Type hints for IDE support and documentation
    ids: List[List[str]]
    documents: List[Optional[List[Optional[str]]]]
    embeddings: List[Optional[List[Optional[List[float]]]]]
    metadatas: List[Optional[List[Optional[Dict[str, Any]]]]]
    scores: List[Optional[List[Optional[float]]]]
    select: List[List[str]]

    def rows(self) -> List[List[SearchResultRow]]:
        """Convert column-major format to row-major format.

        Returns:
            List of lists where each inner list contains SearchResultRow dicts
            for one search payload.
        """
        result: List[List[SearchResultRow]] = []

        # Get all field data with defaults
        all_ids = self.get("ids", [])
        n_payloads = len(all_ids)
        all_docs = self.get("documents") or [None] * n_payloads
        all_embs = self.get("embeddings") or [None] * n_payloads
        all_metas = self.get("metadatas") or [None] * n_payloads
        all_scores = self.get("scores") or [None] * n_payloads

        # Zip payload-level data together
        for ids, docs, embs, metas, scores in zip(
            all_ids, all_docs, all_embs, all_metas, all_scores
        ):
            payload_rows: List[SearchResultRow] = []

            # Zip row-level data together (handle None payloads inline)
            for id_val, doc, emb, meta, score in zip(
                ids,
                docs or [None] * len(ids),
                embs or [None] * len(ids),
                metas or [None] * len(ids),
                scores or [None] * len(ids),
            ):
                row: SearchResultRow = {"id": id_val}

                # Add fields only if they have values
                if doc is not None:
                    row["document"] = doc
                if emb is not None:
                    row["embedding"] = emb
                if meta is not None:
                    row["metadata"] = meta
                if score is not None:
                    row["score"] = score

                payload_rows.append(row)

            result.append(payload_rows)

        return result


class UpdateRequest(TypedDict):
    ids: IDs
    embeddings: Optional[Embeddings]
    metadatas: Optional[Metadatas]
    documents: Optional[Documents]
    uris: Optional[URIs]


# Update result doesn't exist.


class UpsertRequest(TypedDict):
    ids: IDs
    embeddings: Embeddings
    metadatas: Optional[Metadatas]
    documents: Optional[Documents]
    uris: Optional[URIs]


# Upsert result doesn't exist.


class DeleteRequest(TypedDict):
    ids: Optional[IDs]
    where: Optional[Where]
    where_document: Optional[WhereDocument]


# Delete result doesn't exist.


class IndexMetadata(TypedDict):
    dimensionality: int
    # The current number of elements in the index (total = additions - deletes)
    curr_elements: int
    # The auto-incrementing ID of the last inserted element, never decreases so
    # can be used as a count of total historical size. Should increase by 1 every add.
    # Assume cannot overflow
    total_elements_added: int
    time_created: float


Space = Literal["cosine", "l2", "ip"]


# TODO: make warnings prettier and add link to migration docs
@runtime_checkable
class EmbeddingFunction(Protocol[D]):
    """
    A protocol for embedding functions. To implement a new embedding function,
    you need to implement the following methods at minimum:
    - __call__

    For future compatibility, it is strongly recommended to also implement:
    - __init__
    - name
    - build_from_config
    - get_config
    """

    @abstractmethod
    def __call__(self, input: D) -> Embeddings:
        ...

    def embed_query(self, input: D) -> Embeddings:
        """
        Get the embeddings for a query input.
        This method is optional, and if not implemented, the default behavior is to call __call__.
        """
        return self.__call__(input)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: EmbeddingFunction[D], input: D) -> Embeddings:
            result = call(self, input)
            assert result is not None
            return validate_embeddings(cast(Embeddings, normalize_embeddings(result)))

        setattr(cls, "__call__", __call__)

    def embed_with_retries(
        self, input: D, **retry_kwargs: Dict[str, Any]
    ) -> Embeddings:
        return cast(Embeddings, retry(**retry_kwargs)(self.__call__)(input))  # type: ignore[call-overload]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the embedding function.
        Pass any arguments that will be needed to build the embedding function
        config.

        Note: This method is provided for backward compatibility.
        Future implementations should override this method.
        """

        warnings.warn(
            f"The class {self.__class__.__name__} does not implement __init__. "
            "This will be required in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )

    @staticmethod
    def name() -> str:
        """
        Return the name of the embedding function.

        Note: This method is provided for backward compatibility.
        Future implementations should override this method.
        """

        warnings.warn(
            "The EmbeddingFunction class does not implement name(). "
            "This will be required in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        return NotImplemented

    def default_space(self) -> Space:
        """
        Return the default space for the embedding function.
        """
        return "l2"

    def supported_spaces(self) -> List[Space]:
        """
        Return the supported spaces for the embedding function.
        """
        return ["cosine", "l2", "ip"]

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "EmbeddingFunction[D]":
        """
        Build the embedding function from a config, which will be used to
        deserialize the embedding function.

        Note: This method is provided for backward compatibility.
        Future implementations should override this method.
        """

        warnings.warn(
            "The EmbeddingFunction class does not implement build_from_config(). "
            "This will be required in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        return NotImplemented

    def get_config(self) -> Dict[str, Any]:
        """
        Return the config for the embedding function, which will be used to
        serialize the embedding function.

        Note: This method is provided for backward compatibility.
        Future implementations should override this method.
        """

        warnings.warn(
            f"The class {self.__class__.__name__} does not implement get_config(). "
            "This will be required in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        return NotImplemented

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """
        Validate the update to the config.
        """
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the config.
        """
        return

    def is_legacy(self) -> bool:
        if (
            self.name() is NotImplemented
            or self.get_config() is NotImplemented
            or self.build_from_config(self.get_config()) is NotImplemented
        ):
            return True
        return False


class DefaultEmbeddingFunction(EmbeddingFunction[Documents]):
    """Default embedding function that delegates to ONNXMiniLM_L6_V2."""

    def __init__(self) -> None:
        if is_thin_client:
            return

    def __call__(self, input: Documents) -> Embeddings:
        # Import here to avoid circular imports
        from chromadb.utils.embedding_functions.onnx_mini_lm_l6_v2 import (
            ONNXMiniLM_L6_V2,
        )

        return ONNXMiniLM_L6_V2()(input)

    @staticmethod
    def build_from_config(config: Dict[str, Any]) -> "DefaultEmbeddingFunction":
        DefaultEmbeddingFunction.validate_config(config)
        return DefaultEmbeddingFunction()

    @staticmethod
    def name() -> str:
        return "default"

    def get_config(self) -> Dict[str, Any]:
        return {}

    def max_tokens(self) -> int:
        return 256

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        return


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
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, floats, bools, or SparseVectors"""
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
        # Check if value is a SparseVector (validation happens in __post_init__)
        if isinstance(value, SparseVector):
            pass  # Already validated in SparseVector.__post_init__
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        elif not isinstance(value, bool) and not isinstance(
            value, (str, int, float, type(None))
        ):
            raise ValueError(
                f"Expected metadata value to be a str, int, float, bool, SparseVector, or None, got {value} which is a {type(value).__name__}"
            )
    return metadata


def validate_update_metadata(metadata: UpdateMetadata) -> UpdateMetadata:
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, floats, bools, or SparseVectors"""
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
        # Check if value is a SparseVector (validation happens in __post_init__)
        if isinstance(value, SparseVector):
            pass  # Already validated in SparseVector.__post_init__
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        elif not isinstance(value, bool) and not isinstance(
            value, (str, int, float, type(None))
        ):
            raise ValueError(
                f"Expected metadata value to be a str, int, float, bool, SparseVector, or None, got {value}"
            )
    return metadata


def serialize_metadata(metadata: Optional[Metadata]) -> Optional[Dict[str, Any]]:
    """Serialize metadata for transport, converting SparseVector dataclass instances to dicts.

    Args:
        metadata: Metadata dictionary that may contain SparseVector instances

    Returns:
        Metadata dictionary with SparseVector instances converted to transport format
    """
    if metadata is None:
        return None

    result: Dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, SparseVector):
            result[key] = value.to_dict()
        else:
            result[key] = value
    return result


def deserialize_metadata(
    metadata: Optional[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Deserialize metadata from transport, converting dicts with #type=sparse_vector to dataclass instances.

    Args:
        metadata: Metadata dictionary from transport that may contain serialized SparseVectors

    Returns:
        Metadata dictionary with serialized SparseVectors converted to dataclass instances
    """
    if metadata is None:
        return None

    result: Dict[str, Any] = {}
    for key, value in metadata.items():
        if isinstance(value, dict) and value.get(TYPE_KEY) == SPARSE_VECTOR_TYPE_VALUE:
            result[key] = SparseVector.from_dict(value)
        else:
            result[key] = value
    return result


def validate_metadatas(metadatas: Metadatas) -> Metadatas:
    """Validates metadatas to ensure it is a list of dictionaries of strings to strings, ints, floats or bools"""
    if not isinstance(metadatas, list):
        raise ValueError(f"Expected metadatas to be a list, got {metadatas}")
    for metadata in metadatas:
        validate_metadata(metadata)
    return metadatas


def validate_where(where: Where) -> None:
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


def validate_where_document(where_document: WhereDocument) -> None:
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
        if operator not in [
            "$contains",
            "$not_contains",
            "$regex",
            "$not_regex",
            "$and",
            "$or",
        ]:
            raise ValueError(
                f"Expected where document operator to be one of $contains, $not_contains, $regex, $not_regex, $and, $or, got {operator}"
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
        # Value is $contains/$not_contains/$regex/$not_regex operator
        elif not isinstance(operand, str):
            raise ValueError(
                f"Expected where document operand value for operator {operator} to be a str, got {operand}"
            )
        elif len(operand) == 0:
            raise ValueError(
                f"Expected where document operand value for operator {operator} to be a non-empty str"
            )


def validate_include(include: Include, dissalowed: Optional[Include] = None) -> None:
    """Validates include to ensure it is a list of strings. Since get does not allow distances, allow_distances is used
    to control if distances is allowed"""

    if not isinstance(include, list):
        raise ValueError(f"Expected include to be a list, got {include}")
    for item in include:
        if not isinstance(item, str):
            raise ValueError(f"Expected include item to be a str, got {item}")

        # Get the valid items from the Literal type inside the List
        valid_items = get_args(get_args(Include)[0])
        if item not in valid_items:
            raise ValueError(
                f"Expected include item to be one of {', '.join(valid_items)}, got {item}"
            )

        if dissalowed is not None and any(item == e for e in dissalowed):
            raise ValueError(
                f"Include item cannot be one of {', '.join(dissalowed)}, got {item}"
            )


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

        if embedding.dtype not in [
            np.float16,
            np.float32,
            np.float64,
            np.int32,
            np.int64,
        ]:
            raise ValueError(
                "Expected each value in the embedding to be a int or float, got an embedding with "
                f"{embedding.dtype} - {embedding}"
            )
    return embeddings


def validate_sparse_vectors(vectors: SparseVectors) -> SparseVectors:
    """Validates sparse vectors to ensure it is a non-empty list of SparseVector instances.

    This function validates the structure and types of sparse vectors returned by
    SparseEmbeddingFunction implementations. It ensures:
    - Vectors is a list
    - List is non-empty
    - All items are SparseVector instances

    Note: Individual SparseVector validation (sorted indices, non-negative values, etc.)
    happens automatically in SparseVector.__post_init__ when each instance is created.
    This function only validates the list structure and instance types.
    """
    if not isinstance(vectors, list):
        raise ValueError(
            f"Expected sparse vectors to be a list, got {type(vectors).__name__}"
        )
    if len(vectors) == 0:
        raise ValueError(
            f"Expected sparse vectors to be a non-empty list, got {len(vectors)} sparse vectors"
        )
    for i, vector in enumerate(vectors):
        if not isinstance(vector, SparseVector):
            raise ValueError(
                f"Expected SparseVector instance at position {i}, got {type(vector).__name__}"
            )
    return vectors


def validate_documents(documents: Documents, nullable: bool = False) -> None:
    """Validates documents to ensure it is a list of strings"""
    if not isinstance(documents, list):
        raise ValueError(
            f"Expected documents to be a list, got {type(documents).__name__}"
        )
    if len(documents) == 0:
        raise ValueError(
            f"Expected documents to be a non-empty list, got {len(documents)} documents"
        )
    for document in documents:
        # If embeddings are present, some documents can be None
        if document is None and nullable:
            continue
        if not is_document(document):
            raise ValueError(f"Expected document to be a str, got {document}")


def validate_images(images: Images) -> None:
    """Validates images to ensure it is a list of numpy arrays"""
    if not isinstance(images, list):
        raise ValueError(f"Expected images to be a list, got {type(images).__name__}")
    if len(images) == 0:
        raise ValueError(
            f"Expected images to be a non-empty list, got {len(images)} images"
        )
    for image in images:
        if not is_image(image):
            raise ValueError(f"Expected image to be a numpy array, got {image}")


def validate_batch(
    batch: Tuple[
        IDs,
        Optional[Union[Embeddings, PyEmbeddings]],
        Optional[Metadatas],
        Optional[Documents],
        Optional[URIs],
    ],
    limits: Dict[str, Any],
) -> None:
    if len(batch[0]) > limits["max_batch_size"]:
        raise ValueError(
            f"Batch size {len(batch[0])} exceeds maximum batch size {limits['max_batch_size']}"
        )


def convert_np_embeddings_to_list(embeddings: Embeddings) -> PyEmbeddings:
    # Cast the result to PyEmbeddings to ensure type compatibility
    return cast(PyEmbeddings, [embedding.tolist() for embedding in embeddings])


def convert_list_embeddings_to_np(embeddings: PyEmbeddings) -> Embeddings:
    return [np.array(embedding) for embedding in embeddings]


@runtime_checkable
class SparseEmbeddingFunction(Protocol[D]):
    """
    A protocol for sparse vector functions. To implement a new sparse vector function,
    you need to implement the following methods at minimum:
    - __call__

    For future compatibility, it is strongly recommended to also implement:
    - __init__
    - name
    - build_from_config
    - get_config
    """

    @abstractmethod
    def __call__(self, input: D) -> SparseVectors:
        ...

    def embed_query(self, input: D) -> SparseVectors:
        """
        Get the embeddings for a query input.
        This method is optional, and if not implemented, the default behavior is to call __call__.
        """
        return self.__call__(input)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: SparseEmbeddingFunction[D], input: D) -> SparseVectors:
            result = call(self, input)
            assert result is not None
            return validate_sparse_vectors(cast(SparseVectors, result))

        setattr(cls, "__call__", __call__)

    def embed_with_retries(
        self, input: D, **retry_kwargs: Dict[str, Any]
    ) -> SparseVectors:
        return cast(SparseVectors, retry(**retry_kwargs)(self.__call__)(input))  # type: ignore[call-overload]

    @abstractmethod
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the embedding function.
        Pass any arguments that will be needed to build the embedding function
        config.
        """
        ...

    @staticmethod
    @abstractmethod
    def name() -> str:
        """
        Return the name of the embedding function.
        """
        ...

    @staticmethod
    @abstractmethod
    def build_from_config(config: Dict[str, Any]) -> "SparseEmbeddingFunction[D]":
        """
        Build the embedding function from a config, which will be used to
        deserialize the embedding function.
        """
        ...

    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """
        Return the config for the embedding function, which will be used to
        serialize the embedding function.
        """
        ...

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """
        Validate the update to the config.
        """
        return

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        Validate the config.
        """
        return


def validate_sparse_embedding_function(
    sparse_vector_function: SparseEmbeddingFunction[Embeddable],
) -> None:
    """Validate that a sparse vector function conforms to the SparseEmbeddingFunction protocol."""
    function_signature = signature(
        sparse_vector_function.__class__.__call__
    ).parameters.keys()
    protocol_signature = signature(SparseEmbeddingFunction.__call__).parameters.keys()

    if not function_signature == protocol_signature:
        raise ValueError(
            f"Expected SparseEmbeddingFunction.__call__ to have the following signature: {protocol_signature}, got {function_signature}\n"
            "Please see https://docs.trychroma.com/guides/embeddings for details of the SparseEmbeddingFunction interface.\n"
        )


# Index Configuration Types for Collection Schema
def _create_extra_fields_validator(valid_fields: list[str]) -> Any:
    """Create a model validator that provides helpful error messages for invalid fields."""

    @model_validator(mode="before")
    def validate_extra_fields(cls: Type[BaseModel], data: Any) -> Any:
        if isinstance(data, dict):
            invalid_fields = [k for k in data.keys() if k not in valid_fields]
            if invalid_fields:
                invalid_fields_str = ", ".join(f"'{f}'" for f in invalid_fields)
                class_name = cls.__name__
                # Create a clear, actionable error message
                if len(invalid_fields) == 1:
                    msg = (
                        f"'{invalid_fields[0]}' is not a valid field for {class_name}. "
                    )
                else:
                    msg = f"Invalid fields for {class_name}: {invalid_fields_str}. "

                raise PydanticCustomError(
                    "invalid_field",
                    msg,
                    {"invalid_fields": invalid_fields},
                )
        return data

    return validate_extra_fields


class FtsIndexConfig(BaseModel):
    """Configuration for Full-Text Search index. No parameters required."""

    model_config = {"extra": "forbid"}

    pass


class HnswIndexConfig(BaseModel):
    """Configuration for HNSW vector index."""

    _validate_extra_fields = _create_extra_fields_validator(
        [
            "ef_construction",
            "max_neighbors",
            "ef_search",
            "num_threads",
            "batch_size",
            "sync_threshold",
            "resize_factor",
        ]
    )

    ef_construction: Optional[int] = None
    max_neighbors: Optional[int] = None
    ef_search: Optional[int] = None
    num_threads: Optional[int] = None
    batch_size: Optional[int] = None
    sync_threshold: Optional[int] = None
    resize_factor: Optional[float] = None


class SpannIndexConfig(BaseModel):
    """Configuration for SPANN vector index."""

    _validate_extra_fields = _create_extra_fields_validator(
        [
            "search_nprobe",
            "search_rng_factor",
            "search_rng_epsilon",
            "nreplica_count",
            "write_nprobe",
            "write_rng_factor",
            "write_rng_epsilon",
            "split_threshold",
            "num_samples_kmeans",
            "initial_lambda",
            "reassign_neighbor_count",
            "merge_threshold",
            "num_centers_to_merge_to",
            "ef_construction",
            "ef_search",
            "max_neighbors",
        ]
    )

    search_nprobe: Optional[int] = None
    write_nprobe: Optional[int] = None
    ef_construction: Optional[int] = None
    ef_search: Optional[int] = None
    max_neighbors: Optional[int] = None
    reassign_neighbor_count: Optional[int] = None
    split_threshold: Optional[int] = None
    merge_threshold: Optional[int] = None


class VectorIndexConfig(BaseModel):
    """Configuration for vector index with space, embedding function, and algorithm config."""

    model_config = {"arbitrary_types_allowed": True, "extra": "forbid"}

    space: Optional[Space] = None
    embedding_function: Optional[Any] = DefaultEmbeddingFunction()
    source_key: Optional[
        str
    ] = None  # key to source the vector from (accepts str or Key)
    hnsw: Optional[HnswIndexConfig] = None
    spann: Optional[SpannIndexConfig] = None

    @field_validator("source_key", mode="before")
    @classmethod
    def validate_source_key_field(cls, v: Any) -> Optional[str]:
        """Convert Key objects to strings automatically. Accepts both str and Key types."""
        if v is None:
            return None
        # Import Key at runtime to avoid circular import
        from chromadb.execution.expression.operator import Key as KeyType

        if isinstance(v, KeyType):
            v = v.name  # Extract string from Key
        elif isinstance(v, str):
            pass  # Already a string
        else:
            raise ValueError(f"source_key must be str or Key, got {type(v).__name__}")

        # Validate: only #document is allowed if key starts with #
        if v.startswith("#") and v != "#document":
            raise ValueError(
                "source_key cannot begin with '#'. "
                "The only valid key starting with '#' is Key.DOCUMENT or '#document'."
            )

        return v  # type: ignore[no-any-return]

    @field_validator("embedding_function", mode="before")
    @classmethod
    def validate_embedding_function_field(cls, v: Any) -> Any:
        # Use the existing validate_embedding_function for proper validation
        if v is None:
            return v
        if callable(v):
            # Use the existing validation function
            validate_embedding_function(v)
            return v
        raise ValueError("embedding_function must be callable or None")


class SparseVectorIndexConfig(BaseModel):
    """Configuration for sparse vector index."""

    model_config = {"arbitrary_types_allowed": True, "extra": "forbid"}

    # TODO(Sanket): Change this to the appropriate sparse ef and use a default here.
    embedding_function: Optional[Any] = None
    source_key: Optional[
        str
    ] = None  # key to source the sparse vector from (accepts str or Key)
    bm25: Optional[bool] = None

    @field_validator("source_key", mode="before")
    @classmethod
    def validate_source_key_field(cls, v: Any) -> Optional[str]:
        """Convert Key objects to strings automatically. Accepts both str and Key types."""
        if v is None:
            return None
        # Import Key at runtime to avoid circular import
        from chromadb.execution.expression.operator import Key as KeyType

        if isinstance(v, KeyType):
            v = v.name  # Extract string from Key
        elif isinstance(v, str):
            pass  # Already a string
        else:
            raise ValueError(f"source_key must be str or Key, got {type(v).__name__}")

        # Validate: only #document is allowed if key starts with #
        if v.startswith("#") and v != "#document":
            raise ValueError(
                "source_key cannot begin with '#'. "
                "The only valid key starting with '#' is Key.DOCUMENT or '#document'."
            )

        return v  # type: ignore[no-any-return]

    @field_validator("embedding_function", mode="before")
    @classmethod
    def validate_embedding_function_field(cls, v: Any) -> Any:
        # Validate sparse vector function for sparse vector index
        if v is None:
            return v
        if callable(v):
            # Use the sparse vector function validation
            validate_sparse_embedding_function(v)
            return v
        raise ValueError(
            "embedding_function must be a callable SparseEmbeddingFunction or None"
        )


class StringInvertedIndexConfig(BaseModel):
    """Configuration for string inverted index."""

    model_config = {"extra": "forbid"}

    pass


class IntInvertedIndexConfig(BaseModel):
    """Configuration for integer inverted index."""

    model_config = {"extra": "forbid"}

    pass


class FloatInvertedIndexConfig(BaseModel):
    """Configuration for float inverted index."""

    model_config = {"extra": "forbid"}

    pass


class BoolInvertedIndexConfig(BaseModel):
    """Configuration for boolean inverted index."""

    model_config = {"extra": "forbid"}

    pass


# Union type for all index configurations (used by new Schema class)
IndexConfig: TypeAlias = Union[
    FtsIndexConfig,
    VectorIndexConfig,
    SparseVectorIndexConfig,
    StringInvertedIndexConfig,
    IntInvertedIndexConfig,
    FloatInvertedIndexConfig,
    BoolInvertedIndexConfig,
]


# Value type constants
STRING_VALUE_NAME: Final[str] = "string"
INT_VALUE_NAME: Final[str] = "int"
BOOL_VALUE_NAME: Final[str] = "bool"
FLOAT_VALUE_NAME: Final[str] = "float"
FLOAT_LIST_VALUE_NAME: Final[str] = "float_list"
SPARSE_VECTOR_VALUE_NAME: Final[str] = "sparse_vector"

# Index type name constants
FTS_INDEX_NAME: Final[str] = "fts_index"
VECTOR_INDEX_NAME: Final[str] = "vector_index"
SPARSE_VECTOR_INDEX_NAME: Final[str] = "sparse_vector_index"
STRING_INVERTED_INDEX_NAME: Final[str] = "string_inverted_index"
INT_INVERTED_INDEX_NAME: Final[str] = "int_inverted_index"
FLOAT_INVERTED_INDEX_NAME: Final[str] = "float_inverted_index"
BOOL_INVERTED_INDEX_NAME: Final[str] = "bool_inverted_index"
HNSW_INDEX_NAME: Final[str] = "hnsw_index"
SPANN_INDEX_NAME: Final[str] = "spann_index"

# Special key constants
DOCUMENT_KEY: Final[str] = "#document"
EMBEDDING_KEY: Final[str] = "#embedding"
TYPE_KEY: Final[str] = "#type"

# Type value constants
SPARSE_VECTOR_TYPE_VALUE: Final[str] = "sparse_vector"


# Index Type Classes


@dataclass
class FtsIndexType:
    enabled: bool
    config: FtsIndexConfig


@dataclass
class VectorIndexType:
    enabled: bool
    config: VectorIndexConfig


@dataclass
class SparseVectorIndexType:
    enabled: bool
    config: SparseVectorIndexConfig


@dataclass
class StringInvertedIndexType:
    enabled: bool
    config: StringInvertedIndexConfig


@dataclass
class IntInvertedIndexType:
    enabled: bool
    config: IntInvertedIndexConfig


@dataclass
class FloatInvertedIndexType:
    enabled: bool
    config: FloatInvertedIndexConfig


@dataclass
class BoolInvertedIndexType:
    enabled: bool
    config: BoolInvertedIndexConfig


# Individual Value Type Classes


@dataclass
class StringValueType:
    fts_index: Optional[FtsIndexType] = None
    string_inverted_index: Optional[StringInvertedIndexType] = None


@dataclass
class FloatListValueType:
    vector_index: Optional[VectorIndexType] = None


@dataclass
class SparseVectorValueType:
    sparse_vector_index: Optional[SparseVectorIndexType] = None


@dataclass
class IntValueType:
    int_inverted_index: Optional[IntInvertedIndexType] = None


@dataclass
class FloatValueType:
    float_inverted_index: Optional[FloatInvertedIndexType] = None


@dataclass
class BoolValueType:
    bool_inverted_index: Optional[BoolInvertedIndexType] = None


@dataclass
class ValueTypes:
    string: Optional[StringValueType] = None
    float_list: Optional[FloatListValueType] = None
    sparse_vector: Optional[SparseVectorValueType] = None
    int_value: Optional[IntValueType] = None
    float_value: Optional[FloatValueType] = None
    boolean: Optional[BoolValueType] = None


@dataclass
class Schema:
    defaults: ValueTypes
    keys: Dict[str, ValueTypes]

    def __init__(self) -> None:
        # Initialize the dataclass fields first
        self.defaults = ValueTypes()
        self.keys: Dict[str, ValueTypes] = {}

        # Populate with sensible defaults automatically
        self._initialize_defaults()
        self._initialize_keys()

    def create_index(
        self,
        config: Optional[IndexConfig] = None,
        key: Optional[Union[str, "Key"]] = None,
    ) -> "Schema":
        """Create an index configuration."""
        # Convert Key to string if provided
        from chromadb.execution.expression.operator import Key as KeyType

        if key is not None and isinstance(key, KeyType):
            key = key.name

        # Disallow config=None and key=None - too dangerous
        if config is None and key is None:
            raise ValueError(
                "Cannot enable all index types globally. Must specify either config or key."
            )

        # Disallow using special internal keys (#embedding, #document)
        if key is not None and key in (EMBEDDING_KEY, DOCUMENT_KEY):
            raise ValueError(
                f"Cannot create index on special key '{key}'. These keys are managed automatically by the system. Invoke create_index(VectorIndexConfig(...)) without specifying a key to configure the vector index globally."
            )

        # Disallow any key starting with #
        if key is not None and key.startswith("#"):
            raise ValueError(
                "key cannot begin with '#'. "
                "Keys starting with '#' are reserved for system use."
            )

        # Special handling for vector index
        if isinstance(config, VectorIndexConfig):
            if key is None:
                # Allow setting vector config globally - it applies to defaults and #embedding
                # but doesn't change enabled state (vector index is always enabled on #embedding)
                self._set_vector_index_config(config)
                return self
            else:
                # Disallow vector index on any custom key
                raise ValueError(
                    "Vector index cannot be enabled on specific keys. Use create_index(config=VectorIndexConfig(...)) without specifying a key to configure the vector index globally."
                )

        # Special handling for FTS index
        if isinstance(config, FtsIndexConfig):
            if key is None:
                # Allow setting FTS config globally - it applies to defaults and #document
                # but doesn't change enabled state (FTS is always enabled on #document)
                self._set_fts_index_config(config)
                return self
            else:
                # Disallow FTS index on any custom key
                raise ValueError(
                    "FTS index cannot be enabled on specific keys. Use create_index(config=FtsIndexConfig(...)) without specifying a key to configure the FTS index globally."
                )

        # Disallow sparse vector index without a specific key
        if isinstance(config, SparseVectorIndexConfig) and key is None:
            raise ValueError(
                "Sparse vector index must be created on a specific key. "
                "Please specify a key using: create_index(config=SparseVectorIndexConfig(...), key='your_key')"
            )

        # TODO: Consider removing this check in the future to allow enabling all indexes for a key
        # Disallow enabling all index types for a key (config=None, key="some_key")
        if config is None and key is not None:
            raise ValueError(
                f"Cannot enable all index types for key '{key}'. Please specify a specific index configuration."
            )

        # Case 1: config is not None and key is None - enable specific index type globally
        if config is not None and key is None:
            self._set_index_in_defaults(config, enabled=True)

        # Case 2: config is None and key is not None - enable all index types for that key
        elif config is None and key is not None:
            self._enable_all_indexes_for_key(key)

        # Case 3: config is not None and key is not None - enable specific index type for that key
        elif config is not None and key is not None:
            self._set_index_for_key(key, config, enabled=True)

        return self

    def delete_index(
        self,
        config: Optional[IndexConfig] = None,
        key: Optional[Union[str, "Key"]] = None,
    ) -> "Schema":
        """Disable an index configuration (set enabled=False)."""
        # Convert Key to string if provided
        from chromadb.execution.expression.operator import Key as KeyType

        if key is not None and isinstance(key, KeyType):
            key = key.name

        # Case 1: Both config and key are None - fail the request
        if config is None and key is None:
            raise ValueError(
                "Cannot disable all indexes. Must specify either config or key."
            )

        # Disallow using special internal keys (#embedding, #document)
        if key is not None and key in (EMBEDDING_KEY, DOCUMENT_KEY):
            raise ValueError(
                f"Cannot delete index on special key '{key}'. These keys are managed automatically by the system."
            )

        # Disallow any key starting with #
        if key is not None and key.startswith("#"):
            raise ValueError(
                "key cannot begin with '#'. "
                "Keys starting with '#' are reserved for system use."
            )

        # TODO: Consider removing these checks in the future to allow disabling vector, FTS, and sparse vector indexes
        # Temporarily disallow deleting vector index (both globally and per-key)
        if isinstance(config, VectorIndexConfig):
            raise ValueError("Deleting vector index is not currently supported.")

        # Temporarily disallow deleting FTS index (both globally and per-key)
        if isinstance(config, FtsIndexConfig):
            raise ValueError("Deleting FTS index is not currently supported.")

        # Temporarily disallow deleting sparse vector index (both globally and per-key)
        if isinstance(config, SparseVectorIndexConfig):
            raise ValueError("Deleting sparse vector index is not currently supported.")

        # TODO: Consider removing this check in the future to allow disabling all indexes for a key
        # Disallow disabling all index types for a key (config=None, key="some_key")
        if key is not None and config is None:
            raise ValueError(
                f"Cannot disable all index types for key '{key}'. Please specify a specific index configuration."
            )

        # Case 2: key is not None and config is None - disable all possible index types for that key
        if key is not None and config is None:
            self._disable_all_indexes_for_key(key)

        # Case 3: key is not None and config is not None - disable specific index for that key
        elif key is not None and config is not None:
            self._set_index_for_key(key, config, enabled=False)

        # Case 4: key is None and config is not None - disable specific index globally
        elif key is None and config is not None:
            self._set_index_in_defaults(config, enabled=False)

        return self

    def _get_config_class_name(self, config: IndexConfig) -> str:
        """Get the class name for a config."""
        return config.__class__.__name__

    def _set_vector_index_config(self, config: VectorIndexConfig) -> None:
        """
        Set vector index config globally and on #embedding key.
        This updates the config but preserves the enabled state.
        Vector index is always enabled on #embedding, disabled in defaults.
        Note: source_key on #embedding is always preserved as "#document".
        """
        # Update the config in defaults (preserve enabled=False)
        current_enabled = self.defaults.float_list.vector_index.enabled  # type: ignore[union-attr]
        self.defaults.float_list.vector_index = VectorIndexType(enabled=current_enabled, config=config)  # type: ignore[union-attr]

        # Update the config on #embedding key (preserve enabled=True and source_key="#document")
        current_enabled = self.keys[EMBEDDING_KEY].float_list.vector_index.enabled  # type: ignore[union-attr]
        current_source_key = self.keys[EMBEDDING_KEY].float_list.vector_index.config.source_key  # type: ignore[union-attr]

        # Create a new config with user settings but preserve the original source_key
        embedding_config = VectorIndexConfig(
            space=config.space,
            embedding_function=config.embedding_function,
            hnsw=config.hnsw,
            spann=config.spann,
            source_key=current_source_key,  # Preserve original source_key (should be "#document")
        )
        self.keys[EMBEDDING_KEY].float_list.vector_index = VectorIndexType(enabled=current_enabled, config=embedding_config)  # type: ignore[union-attr]

    def _set_fts_index_config(self, config: FtsIndexConfig) -> None:
        """
        Set FTS index config globally and on #document key.
        This updates the config but preserves the enabled state.
        FTS index is always enabled on #document, disabled in defaults.
        """
        # Update the config in defaults (preserve enabled=False)
        current_enabled = self.defaults.string.fts_index.enabled  # type: ignore[union-attr]
        self.defaults.string.fts_index = FtsIndexType(enabled=current_enabled, config=config)  # type: ignore[union-attr]

        # Update the config on #document key (preserve enabled=True)
        current_enabled = self.keys[DOCUMENT_KEY].string.fts_index.enabled  # type: ignore[union-attr]
        self.keys[DOCUMENT_KEY].string.fts_index = FtsIndexType(enabled=current_enabled, config=config)  # type: ignore[union-attr]

    def _set_index_in_defaults(self, config: IndexConfig, enabled: bool) -> None:
        """Set an index configuration in the defaults."""
        config_name = self._get_config_class_name(config)

        if config_name == "FtsIndexConfig":
            if self.defaults.string is None:
                self.defaults.string = StringValueType()
            self.defaults.string.fts_index = FtsIndexType(
                enabled=enabled, config=cast(FtsIndexConfig, config)
            )

        elif config_name == "StringInvertedIndexConfig":
            if self.defaults.string is None:
                self.defaults.string = StringValueType()
            self.defaults.string.string_inverted_index = StringInvertedIndexType(
                enabled=enabled, config=cast(StringInvertedIndexConfig, config)
            )

        elif config_name == "VectorIndexConfig":
            if self.defaults.float_list is None:
                self.defaults.float_list = FloatListValueType()
            self.defaults.float_list.vector_index = VectorIndexType(
                enabled=enabled, config=cast(VectorIndexConfig, config)
            )

        elif config_name == "SparseVectorIndexConfig":
            if self.defaults.sparse_vector is None:
                self.defaults.sparse_vector = SparseVectorValueType()
            self.defaults.sparse_vector.sparse_vector_index = SparseVectorIndexType(
                enabled=enabled, config=cast(SparseVectorIndexConfig, config)
            )

        elif config_name == "IntInvertedIndexConfig":
            if self.defaults.int_value is None:
                self.defaults.int_value = IntValueType()
            self.defaults.int_value.int_inverted_index = IntInvertedIndexType(
                enabled=enabled, config=cast(IntInvertedIndexConfig, config)
            )

        elif config_name == "FloatInvertedIndexConfig":
            if self.defaults.float_value is None:
                self.defaults.float_value = FloatValueType()
            self.defaults.float_value.float_inverted_index = FloatInvertedIndexType(
                enabled=enabled, config=cast(FloatInvertedIndexConfig, config)
            )

        elif config_name == "BoolInvertedIndexConfig":
            if self.defaults.boolean is None:
                self.defaults.boolean = BoolValueType()
            self.defaults.boolean.bool_inverted_index = BoolInvertedIndexType(
                enabled=enabled, config=cast(BoolInvertedIndexConfig, config)
            )

    def _validate_single_sparse_vector_index(self, key: str) -> None:
        """
        Validate that only one sparse vector index is enabled per collection.

        Raises ValueError if another key already has a sparse vector index enabled.
        """
        for existing_key, value_types in self.keys.items():
            if existing_key == key:
                continue  # Skip the current key being updated
            if value_types.sparse_vector is not None:
                if value_types.sparse_vector.sparse_vector_index is not None:
                    if value_types.sparse_vector.sparse_vector_index.enabled:
                        raise ValueError(
                            f"Cannot enable sparse vector index on key '{key}'. "
                            f"A sparse vector index is already enabled on key '{existing_key}'. "
                            f"Only one sparse vector index is allowed per collection."
                        )

    def _validate_sparse_vector_config(self, config: SparseVectorIndexConfig) -> None:
        """
        Validate that if source_key is provided then embedding_function is also provided
        since there is no default embedding function. Raises ValueError otherwise.
        """
        if config.source_key is not None and config.embedding_function is None:
            raise ValueError(
                f"If source_key is provided then embedding_function must also be provided "
                f"since there is no default embedding function. Config: {config}"
            )

    def _set_index_for_key(self, key: str, config: IndexConfig, enabled: bool) -> None:
        """Set an index configuration for a specific key."""
        config_name = self._get_config_class_name(config)

        # Validate sparse vector index - only one is allowed per collection
        # Do this BEFORE creating the key entry
        if config_name == "SparseVectorIndexConfig" and enabled:
            self._validate_single_sparse_vector_index(key)
            self._validate_sparse_vector_config(cast(SparseVectorIndexConfig, config))

        if key not in self.keys:
            self.keys[key] = ValueTypes()

        if config_name == "FtsIndexConfig":
            if self.keys[key].string is None:
                self.keys[key].string = StringValueType()
            self.keys[key].string.fts_index = FtsIndexType(enabled=enabled, config=cast(FtsIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "StringInvertedIndexConfig":
            if self.keys[key].string is None:
                self.keys[key].string = StringValueType()
            self.keys[key].string.string_inverted_index = StringInvertedIndexType(enabled=enabled, config=cast(StringInvertedIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "VectorIndexConfig":
            if self.keys[key].float_list is None:
                self.keys[key].float_list = FloatListValueType()
            self.keys[key].float_list.vector_index = VectorIndexType(enabled=enabled, config=cast(VectorIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "SparseVectorIndexConfig":
            if self.keys[key].sparse_vector is None:
                self.keys[key].sparse_vector = SparseVectorValueType()
            self.keys[key].sparse_vector.sparse_vector_index = SparseVectorIndexType(enabled=enabled, config=cast(SparseVectorIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "IntInvertedIndexConfig":
            if self.keys[key].int_value is None:
                self.keys[key].int_value = IntValueType()
            self.keys[key].int_value.int_inverted_index = IntInvertedIndexType(enabled=enabled, config=cast(IntInvertedIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "FloatInvertedIndexConfig":
            if self.keys[key].float_value is None:
                self.keys[key].float_value = FloatValueType()
            self.keys[key].float_value.float_inverted_index = FloatInvertedIndexType(enabled=enabled, config=cast(FloatInvertedIndexConfig, config))  # type: ignore[union-attr]

        elif config_name == "BoolInvertedIndexConfig":
            if self.keys[key].boolean is None:
                self.keys[key].boolean = BoolValueType()
            self.keys[key].boolean.bool_inverted_index = BoolInvertedIndexType(enabled=enabled, config=cast(BoolInvertedIndexConfig, config))  # type: ignore[union-attr]

    def _enable_all_indexes_for_key(self, key: str) -> None:
        """Enable all possible index types for a specific key."""
        if key not in self.keys:
            self.keys[key] = ValueTypes()

        self._validate_single_sparse_vector_index(key)

        # Enable all index types with default configs
        self.keys[key].string = StringValueType(
            fts_index=FtsIndexType(enabled=True, config=FtsIndexConfig()),
            string_inverted_index=StringInvertedIndexType(
                enabled=True, config=StringInvertedIndexConfig()
            ),
        )
        self.keys[key].float_list = FloatListValueType(
            vector_index=VectorIndexType(enabled=True, config=VectorIndexConfig())
        )
        self.keys[key].sparse_vector = SparseVectorValueType(
            sparse_vector_index=SparseVectorIndexType(
                enabled=True, config=SparseVectorIndexConfig()
            )
        )
        self.keys[key].int_value = IntValueType(
            int_inverted_index=IntInvertedIndexType(
                enabled=True, config=IntInvertedIndexConfig()
            )
        )
        self.keys[key].float_value = FloatValueType(
            float_inverted_index=FloatInvertedIndexType(
                enabled=True, config=FloatInvertedIndexConfig()
            )
        )
        self.keys[key].boolean = BoolValueType(
            bool_inverted_index=BoolInvertedIndexType(
                enabled=True, config=BoolInvertedIndexConfig()
            )
        )

    def _disable_all_indexes_for_key(self, key: str) -> None:
        """Disable all possible index types for a specific key."""
        if key not in self.keys:
            self.keys[key] = ValueTypes()

        # Disable all index types with default configs
        self.keys[key].string = StringValueType(
            fts_index=FtsIndexType(enabled=False, config=FtsIndexConfig()),
            string_inverted_index=StringInvertedIndexType(
                enabled=False, config=StringInvertedIndexConfig()
            ),
        )
        self.keys[key].float_list = FloatListValueType(
            vector_index=VectorIndexType(enabled=False, config=VectorIndexConfig())
        )
        self.keys[key].sparse_vector = SparseVectorValueType(
            sparse_vector_index=SparseVectorIndexType(
                enabled=False, config=SparseVectorIndexConfig()
            )
        )
        self.keys[key].int_value = IntValueType(
            int_inverted_index=IntInvertedIndexType(
                enabled=False, config=IntInvertedIndexConfig()
            )
        )
        self.keys[key].float_value = FloatValueType(
            float_inverted_index=FloatInvertedIndexType(
                enabled=False, config=FloatInvertedIndexConfig()
            )
        )
        self.keys[key].boolean = BoolValueType(
            bool_inverted_index=BoolInvertedIndexType(
                enabled=False, config=BoolInvertedIndexConfig()
            )
        )

    def _initialize_defaults(self) -> None:
        """Initialize defaults with base structure and standard configuration."""
        # Initialize all value types with default configurations
        self.defaults.string = StringValueType(
            fts_index=FtsIndexType(
                enabled=False, config=FtsIndexConfig()
            ),  # Disabled for performance
            string_inverted_index=StringInvertedIndexType(
                enabled=True, config=StringInvertedIndexConfig()
            ),
        )

        self.defaults.float_list = FloatListValueType(
            vector_index=VectorIndexType(
                enabled=False, config=VectorIndexConfig()
            )  # Disabled by default
        )

        self.defaults.sparse_vector = SparseVectorValueType(
            sparse_vector_index=SparseVectorIndexType(
                enabled=False, config=SparseVectorIndexConfig()
            )  # Disabled for performance
        )

        self.defaults.int_value = IntValueType(
            int_inverted_index=IntInvertedIndexType(
                enabled=True, config=IntInvertedIndexConfig()
            )
        )

        self.defaults.float_value = FloatValueType(
            float_inverted_index=FloatInvertedIndexType(
                enabled=True, config=FloatInvertedIndexConfig()
            )
        )

        self.defaults.boolean = BoolValueType(
            bool_inverted_index=BoolInvertedIndexType(
                enabled=True, config=BoolInvertedIndexConfig()
            )
        )

    def _initialize_keys(self) -> None:
        """Initialize key-specific index overrides."""
        # Enable full-text search for document content
        self.keys[DOCUMENT_KEY] = ValueTypes(
            string=StringValueType(
                fts_index=FtsIndexType(enabled=True, config=FtsIndexConfig()),
                string_inverted_index=StringInvertedIndexType(
                    enabled=False, config=StringInvertedIndexConfig()
                ),
            )
        )

        # Enable vector index for embeddings with document source reference
        vector_config = VectorIndexConfig(source_key=DOCUMENT_KEY)
        self.keys[EMBEDDING_KEY] = ValueTypes(
            float_list=FloatListValueType(
                vector_index=VectorIndexType(enabled=True, config=vector_config)
            )
        )

    def serialize_to_json(self) -> Dict[str, Any]:
        """Convert Schema to a JSON-serializable dict for transmission over the wire."""
        # Convert defaults to JSON format
        defaults_json = self._serialize_value_types(self.defaults)

        # Convert key overrides to JSON format
        keys_json: Dict[str, Dict[str, Any]] = {}
        for key, value_types in self.keys.items():
            keys_json[key] = self._serialize_value_types(value_types)

        return {"defaults": defaults_json, "keys": keys_json}

    @classmethod
    def deserialize_from_json(cls, json_data: Dict[str, Any]) -> "Schema":
        """Create Schema from JSON-serialized data."""
        # Create empty instance
        instance = cls.__new__(cls)

        # Deserialize and set the components
        instance.defaults = cls._deserialize_value_types(json_data.get("defaults", {}))
        instance.keys = {}
        for key, value_types_json in json_data.get("keys", {}).items():
            instance.keys[key] = cls._deserialize_value_types(value_types_json)

        return instance

    def _serialize_value_types(self, value_types: ValueTypes) -> Dict[str, Any]:
        """Convert a ValueTypes object to JSON-serializable format."""
        result: Dict[str, Any] = {}

        # Serialize each value type if it exists
        if value_types.string is not None:
            result[STRING_VALUE_NAME] = self._serialize_string_value_type(
                value_types.string
            )

        if value_types.float_list is not None:
            result[FLOAT_LIST_VALUE_NAME] = self._serialize_float_list_value_type(
                value_types.float_list
            )

        if value_types.sparse_vector is not None:
            result[SPARSE_VECTOR_VALUE_NAME] = self._serialize_sparse_vector_value_type(
                value_types.sparse_vector
            )

        if value_types.int_value is not None:
            result[INT_VALUE_NAME] = self._serialize_int_value_type(
                value_types.int_value
            )

        if value_types.float_value is not None:
            result[FLOAT_VALUE_NAME] = self._serialize_float_value_type(
                value_types.float_value
            )

        if value_types.boolean is not None:
            result[BOOL_VALUE_NAME] = self._serialize_bool_value_type(
                value_types.boolean
            )

        return result

    def _serialize_string_value_type(
        self, string_type: StringValueType
    ) -> Dict[str, Any]:
        """Serialize StringValueType to JSON format."""
        result: Dict[str, Any] = {}

        if string_type.fts_index is not None:
            result[FTS_INDEX_NAME] = {
                "enabled": string_type.fts_index.enabled,
                "config": self._serialize_config(string_type.fts_index.config),
            }

        if string_type.string_inverted_index is not None:
            result[STRING_INVERTED_INDEX_NAME] = {
                "enabled": string_type.string_inverted_index.enabled,
                "config": self._serialize_config(
                    string_type.string_inverted_index.config
                ),
            }

        return result

    def _serialize_float_list_value_type(
        self, float_list_type: FloatListValueType
    ) -> Dict[str, Any]:
        """Serialize FloatListValueType to JSON format."""
        result: Dict[str, Any] = {}

        if float_list_type.vector_index is not None:
            result[VECTOR_INDEX_NAME] = {
                "enabled": float_list_type.vector_index.enabled,
                "config": self._serialize_config(float_list_type.vector_index.config),
            }

        return result

    def _serialize_sparse_vector_value_type(
        self, sparse_vector_type: SparseVectorValueType
    ) -> Dict[str, Any]:
        """Serialize SparseVectorValueType to JSON format."""
        result: Dict[str, Any] = {}

        if sparse_vector_type.sparse_vector_index is not None:
            result[SPARSE_VECTOR_INDEX_NAME] = {
                "enabled": sparse_vector_type.sparse_vector_index.enabled,
                "config": self._serialize_config(
                    sparse_vector_type.sparse_vector_index.config
                ),
            }

        return result

    def _serialize_int_value_type(self, int_type: IntValueType) -> Dict[str, Any]:
        """Serialize IntValueType to JSON format."""
        result: Dict[str, Any] = {}

        if int_type.int_inverted_index is not None:
            result[INT_INVERTED_INDEX_NAME] = {
                "enabled": int_type.int_inverted_index.enabled,
                "config": self._serialize_config(int_type.int_inverted_index.config),
            }

        return result

    def _serialize_float_value_type(self, float_type: FloatValueType) -> Dict[str, Any]:
        """Serialize FloatValueType to JSON format."""
        result: Dict[str, Any] = {}

        if float_type.float_inverted_index is not None:
            result[FLOAT_INVERTED_INDEX_NAME] = {
                "enabled": float_type.float_inverted_index.enabled,
                "config": self._serialize_config(
                    float_type.float_inverted_index.config
                ),
            }

        return result

    def _serialize_bool_value_type(self, bool_type: BoolValueType) -> Dict[str, Any]:
        """Serialize BoolValueType to JSON format."""
        result: Dict[str, Any] = {}

        if bool_type.bool_inverted_index is not None:
            result[BOOL_INVERTED_INDEX_NAME] = {
                "enabled": bool_type.bool_inverted_index.enabled,
                "config": self._serialize_config(bool_type.bool_inverted_index.config),
            }

        return result

    def _serialize_config(self, config: IndexConfig) -> Dict[str, Any]:
        """Serialize config object to JSON-serializable dictionary."""
        # All IndexConfig types are Pydantic models, so use model_dump
        config_dict = config.model_dump(exclude_none=True)

        # Handle embedding function serialization for vector configs
        if isinstance(config, VectorIndexConfig):
            if hasattr(config, "embedding_function"):
                embedding_func = getattr(config, "embedding_function", None)
                if embedding_func is None:
                    config_dict["embedding_function"] = {"type": "legacy"}
                else:
                    try:
                        # Cast to EmbeddingFunction type to access methods
                        embedding_func = cast(EmbeddingFunction, embedding_func)  # type: ignore
                        if embedding_func.is_legacy():
                            config_dict["embedding_function"] = {"type": "legacy"}
                        else:
                            if hasattr(embedding_func, "validate_config"):
                                embedding_func.validate_config(
                                    embedding_func.get_config()
                                )
                            config_dict["embedding_function"] = {
                                "name": embedding_func.name(),
                                "type": "known",
                                "config": embedding_func.get_config(),
                            }

                            # Handle space resolution from embedding function
                            if hasattr(config, "space") and config.space is None:
                                config_dict["space"] = embedding_func.default_space()
                            elif hasattr(config, "space") and config.space is not None:
                                if (
                                    config.space
                                    not in embedding_func.supported_spaces()
                                ):
                                    warnings.warn(
                                        f"space {config.space} is not supported by {embedding_func.name()}. Supported spaces: {embedding_func.supported_spaces()}",
                                        UserWarning,
                                        stacklevel=2,
                                    )
                    except Exception:
                        config_dict["embedding_function"] = {"type": "legacy"}

        elif isinstance(config, SparseVectorIndexConfig):
            if hasattr(config, "embedding_function"):
                embedding_func = getattr(config, "embedding_function", None)
                if embedding_func is None:
                    config_dict["embedding_function"] = {"type": "unknown"}
                else:
                    embedding_func = cast(SparseEmbeddingFunction, embedding_func)  # type: ignore
                    if hasattr(embedding_func, "validate_config"):
                        embedding_func.validate_config(embedding_func.get_config())
                    config_dict["embedding_function"] = {
                        "name": embedding_func.name(),
                        "type": "known",
                        "config": embedding_func.get_config(),
                    }

        return config_dict

    @classmethod
    def _deserialize_value_types(cls, value_types_json: Dict[str, Any]) -> ValueTypes:
        """Deserialize ValueTypes from JSON format."""
        result = ValueTypes()

        # Deserialize each value type if present
        if STRING_VALUE_NAME in value_types_json:
            result.string = cls._deserialize_string_value_type(
                value_types_json[STRING_VALUE_NAME]
            )

        if FLOAT_LIST_VALUE_NAME in value_types_json:
            result.float_list = cls._deserialize_float_list_value_type(
                value_types_json[FLOAT_LIST_VALUE_NAME]
            )

        if SPARSE_VECTOR_VALUE_NAME in value_types_json:
            result.sparse_vector = cls._deserialize_sparse_vector_value_type(
                value_types_json[SPARSE_VECTOR_VALUE_NAME]
            )

        if INT_VALUE_NAME in value_types_json:
            result.int_value = cls._deserialize_int_value_type(
                value_types_json[INT_VALUE_NAME]
            )

        if FLOAT_VALUE_NAME in value_types_json:
            result.float_value = cls._deserialize_float_value_type(
                value_types_json[FLOAT_VALUE_NAME]
            )

        if BOOL_VALUE_NAME in value_types_json:
            result.boolean = cls._deserialize_bool_value_type(
                value_types_json[BOOL_VALUE_NAME]
            )

        return result

    @classmethod
    def _deserialize_string_value_type(
        cls, string_json: Dict[str, Any]
    ) -> StringValueType:
        """Deserialize StringValueType from JSON format."""
        fts_index = None
        string_inverted_index = None

        if FTS_INDEX_NAME in string_json:
            fts_index_data = string_json[FTS_INDEX_NAME]
            fts_enabled = fts_index_data.get("enabled", True)
            fts_config_data = fts_index_data.get("config", {})
            fts_config = FtsIndexConfig(**fts_config_data)
            fts_index = FtsIndexType(enabled=fts_enabled, config=fts_config)

        if STRING_INVERTED_INDEX_NAME in string_json:
            string_index_data = string_json[STRING_INVERTED_INDEX_NAME]
            string_enabled = string_index_data.get("enabled", True)
            string_config_data = string_index_data.get("config", {})
            string_config = StringInvertedIndexConfig(**string_config_data)
            string_inverted_index = StringInvertedIndexType(
                enabled=string_enabled, config=string_config
            )

        return StringValueType(
            fts_index=fts_index, string_inverted_index=string_inverted_index
        )

    @classmethod
    def _deserialize_float_list_value_type(
        cls, float_list_json: Dict[str, Any]
    ) -> FloatListValueType:
        """Deserialize FloatListValueType from JSON format."""
        vector_index = None

        if VECTOR_INDEX_NAME in float_list_json:
            index_data = float_list_json[VECTOR_INDEX_NAME]
            enabled = index_data.get("enabled", True)
            config_data = deepcopy(index_data.get("config", {}))

            # Handle embedding function deserialization
            if "embedding_function" in config_data:
                ef_config = config_data["embedding_function"]
                if ef_config.get("type") == "legacy":
                    config_data["embedding_function"] = None
                else:
                    try:
                        from chromadb.utils.embedding_functions import (
                            known_embedding_functions,
                        )

                        ef_name = ef_config["name"]
                        ef = known_embedding_functions[ef_name]
                        config_data["embedding_function"] = ef.build_from_config(
                            ef_config["config"]
                        )

                        # Handle space deserialization
                        if "space" not in config_data or config_data["space"] is None:
                            config_data["space"] = config_data[
                                "embedding_function"
                            ].default_space()
                    except Exception as e:
                        warnings.warn(
                            f"Could not reconstruct embedding function {ef_config.get('name', 'unknown')}: {e}. Setting to None.",
                            UserWarning,
                            stacklevel=2,
                        )
                        config_data["embedding_function"] = None

            config = VectorIndexConfig(**config_data)
            vector_index = VectorIndexType(enabled=enabled, config=config)

        return FloatListValueType(vector_index=vector_index)

    @classmethod
    def _deserialize_sparse_vector_value_type(
        cls, sparse_vector_json: Dict[str, Any]
    ) -> SparseVectorValueType:
        """Deserialize SparseVectorValueType from JSON format."""
        sparse_vector_index = None

        if SPARSE_VECTOR_INDEX_NAME in sparse_vector_json:
            index_data = sparse_vector_json[SPARSE_VECTOR_INDEX_NAME]
            enabled = index_data.get("enabled", True)
            config_data = deepcopy(index_data.get("config", {}))

            # Handle embedding function deserialization
            if "embedding_function" in config_data:
                ef_config = config_data["embedding_function"]
                if (
                    ef_config.get("type") == "unknown"
                    or ef_config.get("type") == "legacy"
                ):
                    config_data["embedding_function"] = None
                else:
                    try:
                        from chromadb.utils.embedding_functions import (
                            sparse_known_embedding_functions,
                        )

                        ef_name = ef_config["name"]
                        ef = sparse_known_embedding_functions[ef_name]
                        config_data["embedding_function"] = ef.build_from_config(
                            ef_config["config"]
                        )
                    except Exception as e:
                        warnings.warn(
                            f"Could not reconstruct sparse embedding function {ef_config.get('name', 'unknown')}: {e}. Setting to None.",
                            UserWarning,
                            stacklevel=2,
                        )
                        config_data["embedding_function"] = None

            config = SparseVectorIndexConfig(**config_data)
            sparse_vector_index = SparseVectorIndexType(enabled=enabled, config=config)

        return SparseVectorValueType(sparse_vector_index=sparse_vector_index)

    @classmethod
    def _deserialize_int_value_type(cls, int_json: Dict[str, Any]) -> IntValueType:
        """Deserialize IntValueType from JSON format."""
        int_inverted_index = None

        if INT_INVERTED_INDEX_NAME in int_json:
            index_data = int_json[INT_INVERTED_INDEX_NAME]
            enabled = index_data.get("enabled", True)
            config_data = index_data.get("config", {})
            config = IntInvertedIndexConfig(**config_data)
            int_inverted_index = IntInvertedIndexType(enabled=enabled, config=config)

        return IntValueType(int_inverted_index=int_inverted_index)

    @classmethod
    def _deserialize_float_value_type(
        cls, float_json: Dict[str, Any]
    ) -> FloatValueType:
        """Deserialize FloatValueType from JSON format."""
        float_inverted_index = None

        if FLOAT_INVERTED_INDEX_NAME in float_json:
            index_data = float_json[FLOAT_INVERTED_INDEX_NAME]
            enabled = index_data.get("enabled", True)
            config_data = index_data.get("config", {})
            config = FloatInvertedIndexConfig(**config_data)
            float_inverted_index = FloatInvertedIndexType(
                enabled=enabled, config=config
            )

        return FloatValueType(float_inverted_index=float_inverted_index)

    @classmethod
    def _deserialize_bool_value_type(cls, bool_json: Dict[str, Any]) -> BoolValueType:
        """Deserialize BoolValueType from JSON format."""
        bool_inverted_index = None

        if BOOL_INVERTED_INDEX_NAME in bool_json:
            index_data = bool_json[BOOL_INVERTED_INDEX_NAME]
            enabled = index_data.get("enabled", True)
            config_data = index_data.get("config", {})
            config = BoolInvertedIndexConfig(**config_data)
            bool_inverted_index = BoolInvertedIndexType(enabled=enabled, config=config)

        return BoolValueType(bool_inverted_index=bool_inverted_index)
