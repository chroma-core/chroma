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
    ClassVar,
    Literal,
    get_args,
    TYPE_CHECKING,
    Final,
)
from numpy.typing import NDArray
import numpy as np
from typing_extensions import TypedDict, Protocol, runtime_checkable
from pydantic import BaseModel, field_validator

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
    pass
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
    "is_valid_sparse_vector",
    "validate_sparse_vector",
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
    "IndexEntry",
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
    # Internal Index Types
    "InternalFtsIndex",
    "InternalHnswIndex",
    "InternalSpannIndex",
    "InternalVectorIndex",
    "InternalSparseVectorIndex",
    "InternalStringInvertedIndex",
    "InternalIntInvertedIndex",
    "InternalFloatInvertedIndex",
    "InternalBoolInvertedIndex",
    "InternalIndexType",
    "ValueTypeIndexes",
    # Schema Builder and Internal Schema
    "Schema",
    "InternalSchema",
    # Space type
    "Space",
    # Embedding Functions
    "EmbeddingFunction",
    "SparseEmbeddingFunction",
    "validate_embedding_function",
    "validate_sparse_embedding_function",
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
SparseEmbedding = SparseVector
SparseEmbeddings = List[SparseEmbedding]


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
        return pybase64.b64encode_as_string(  # type: ignore
            _get_struct(len(embedding)).pack(*embedding)
        )
    except OverflowError:
        return pybase64.b64encode_as_string(  # type: ignore
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
    """
    base_record_set = normalize_base_record_set(
        embeddings=embeddings, documents=documents, images=images, uris=uris
    )

    return InsertRecordSet(
        ids=cast(IDs, maybe_cast_one_to_many(ids)),
        metadatas=maybe_cast_one_to_many(metadatas),
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


class SearchResult(dict):
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
        return cast(Embeddings, retry(**retry_kwargs)(self.__call__)(input))

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the embedding function.
        Pass any arguments that will be needed to build the embedding function
        config.

        Note: This method is provided for backward compatibility.
        Future implementations should override this method.
        """
        import warnings

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
        import warnings

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
        import warnings

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
        import warnings

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


def validate_sparse_embedding_function(
    sparse_embedding_function: Any,
) -> None:
    """Validate that a sparse embedding function conforms to the SparseEmbeddingFunction protocol."""
    if not callable(sparse_embedding_function):
        raise ValueError('sparse_embedding_function must be callable')

    if not hasattr(sparse_embedding_function, '__call__'):
        raise ValueError('sparse_embedding_function must have a __call__ method')

    # Basic validation - check if it looks like a sparse embedding function
    # We'll do more detailed validation when SparseEmbeddingFunction is fully defined


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


def is_valid_sparse_vector(value: Any) -> bool:
    """Check if a value looks like a SparseVector (has indices and values keys)."""
    return isinstance(value, dict) and "indices" in value and "values" in value


def validate_sparse_vector(value: Any) -> None:
    """Validate that a value is a properly formed SparseVector.

    Args:
        value: The value to validate as a SparseVector

    Raises:
        ValueError: If the value is not a valid SparseVector
    """
    if not isinstance(value, dict):
        raise ValueError(
            f"Expected SparseVector to be a dict, got {type(value).__name__}"
        )

    if "indices" not in value or "values" not in value:
        raise ValueError("SparseVector must have 'indices' and 'values' keys")

    indices = value.get("indices")
    values = value.get("values")

    # Validate indices
    if not isinstance(indices, list):
        raise ValueError(
            f"Expected SparseVector indices to be a list, got {type(indices).__name__}"
        )

    # Validate values
    if not isinstance(values, list):
        raise ValueError(
            f"Expected SparseVector values to be a list, got {type(values).__name__}"
        )

    # Check lengths match
    if len(indices) != len(values):
        raise ValueError(
            f"SparseVector indices and values must have the same length, "
            f"got {len(indices)} indices and {len(values)} values"
        )

    # Validate each index
    for i, idx in enumerate(indices):
        if not isinstance(idx, int):
            raise ValueError(
                f"SparseVector indices must be integers, got {type(idx).__name__} at position {i}"
            )
        if idx < 0:
            raise ValueError(
                f"SparseVector indices must be non-negative, got {idx} at position {i}"
            )

    # Validate each value
    for i, val in enumerate(values):
        if not isinstance(val, (int, float)):
            raise ValueError(
                f"SparseVector values must be numbers, got {type(val).__name__} at position {i}"
            )


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
        # Check if value is a SparseVector
        if is_valid_sparse_vector(value):
            try:
                validate_sparse_vector(value)
            except ValueError as e:
                raise ValueError(f"Invalid SparseVector for key '{key}': {e}")
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
        # Check if value is a SparseVector
        if is_valid_sparse_vector(value):
            try:
                validate_sparse_vector(value)
            except ValueError as e:
                raise ValueError(f"Invalid SparseVector for key '{key}': {e}")
        # isinstance(True, int) evaluates to True, so we need to check for bools separately
        elif not isinstance(value, bool) and not isinstance(
            value, (str, int, float, type(None))
        ):
            raise ValueError(
                f"Expected metadata value to be a str, int, float, bool, SparseVector, or None, got {value}"
            )
    return metadata


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


def validate_sparse_embeddings(embeddings: SparseEmbeddings) -> SparseEmbeddings:
    """Validates sparse embeddings to ensure it is a list of sparse vectors"""
    if not isinstance(embeddings, list):
        raise ValueError(
            f"Expected sparse embeddings to be a list, got {type(embeddings).__name__}"
        )
    if len(embeddings) == 0:
        raise ValueError(
            f"Expected sparse embeddings to be a non-empty list, got {len(embeddings)} sparse embeddings"
        )
    for embedding in embeddings:
        validate_sparse_vector(embedding)
    return embeddings


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
    A protocol for sparse embedding functions. To implement a new sparse embedding function,
    you need to implement the following methods at minimum:
    - __call__

    For future compatibility, it is strongly recommended to also implement:
    - __init__
    - name
    - build_from_config
    - get_config
    """

    @abstractmethod
    def __call__(self, input: D) -> SparseEmbeddings:
        ...

    def embed_query(self, input: D) -> SparseEmbeddings:
        """
        Get the embeddings for a query input.
        This method is optional, and if not implemented, the default behavior is to call __call__.
        """
        return self.__call__(input)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: SparseEmbeddingFunction[D], input: D) -> SparseEmbeddings:
            result = call(self, input)
            assert result is not None
            return validate_sparse_embeddings(cast(SparseEmbeddings, result))

        setattr(cls, "__call__", __call__)

    def embed_with_retries(
        self, input: D, **retry_kwargs: Dict[str, Any]
    ) -> SparseEmbeddings:
        return cast(SparseEmbeddings, retry(**retry_kwargs)(self.__call__)(input))

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


# Index Configuration Types for Collection Schema
class FtsIndexConfig(BaseModel):
    """Configuration for Full-Text Search index. No parameters required."""
    pass


class HnswIndexConfig(BaseModel):
    """Configuration for HNSW vector index."""
    ef_construction: Optional[int] = None
    max_neighbors: Optional[int] = None
    ef_search: Optional[int] = None
    num_threads: Optional[int] = None
    batch_size: Optional[int] = None
    sync_threshold: Optional[int] = None
    resize_factor: Optional[float] = None


class SpannIndexConfig(BaseModel):
    """Configuration for SPANN vector index."""
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
    model_config = {"arbitrary_types_allowed": True}
    space: Optional[Space] = None
    embedding_function: Optional[Any] = None
    source_key: Optional[str] = None  # key to source the vector from
    hnsw: Optional[HnswIndexConfig] = None
    spann: Optional[SpannIndexConfig] = None

    @field_validator('embedding_function', mode='before')
    @classmethod
    def validate_embedding_function_field(cls, v: Any) -> Any:
        # Use the existing validate_embedding_function for proper validation
        if v is None:
            return v
        if callable(v):
            # Use the existing validation function
            validate_embedding_function(v)
            return v
        raise ValueError('embedding_function must be callable or None')


class SparseVectorIndexConfig(BaseModel):
    """Configuration for sparse vector index."""
    model_config = {"arbitrary_types_allowed": True}
    embedding_function: Optional[Any] = None
    source_key: Optional[str] = None  # key to source the sparse vector from

    @field_validator('embedding_function', mode='before')
    @classmethod
    def validate_embedding_function_field(cls, v: Any) -> Any:
        # Validate sparse embedding function for sparse vector index
        if v is None:
            return v
        if callable(v):
            # Use the sparse embedding function validation
            validate_sparse_embedding_function(v)
            return v
        raise ValueError('embedding_function must be a callable SparseEmbeddingFunction or None')


class StringInvertedIndexConfig(BaseModel):
    """Configuration for string inverted index."""
    pass


class IntInvertedIndexConfig(BaseModel):
    """Configuration for integer inverted index."""
    pass


class FloatInvertedIndexConfig(BaseModel):
    """Configuration for float inverted index."""
    pass


class BoolInvertedIndexConfig(BaseModel):
    """Configuration for boolean inverted index."""
    pass


# Value type constants
STRING_VALUE_NAME: Final[str] = "#string"
INT_VALUE_NAME: Final[str] = "#int"
BOOL_VALUE_NAME: Final[str] = "#bool"
FLOAT_VALUE_NAME: Final[str] = "#float"
FLOAT_LIST_VALUE_NAME: Final[str] = "#float_list"
SPARSE_VECTOR_VALUE_NAME: Final[str] = "#sparse_vector"

# Index type name constants
FTS_INDEX_NAME: Final[str] = "$fts_index"
VECTOR_INDEX_NAME: Final[str] = "$vector_index"
SPARSE_VECTOR_INDEX_NAME: Final[str] = "$sparse_vector_index"
STRING_INVERTED_INDEX_NAME: Final[str] = "$string_inverted_index"
INT_INVERTED_INDEX_NAME: Final[str] = "$int_inverted_index"
FLOAT_INVERTED_INDEX_NAME: Final[str] = "$float_inverted_index"
BOOL_INVERTED_INDEX_NAME: Final[str] = "$bool_inverted_index"
HNSW_INDEX_NAME: Final[str] = "$hnsw_index"
SPANN_INDEX_NAME: Final[str] = "$spann_index"

# Special key constants
DOCUMENT_KEY: Final[str] = "$document"
EMBEDDING_KEY: Final[str] = "$embedding"


# Internal index types that encapsulate the configuration, name, value type, and enabled status
class InternalFtsIndex:
    """Internal wrapper for FTS index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$fts_index"
    VALUE_TYPE_NAME: Final[str] = STRING_VALUE_NAME

    def __init__(self, config: FtsIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalHnswIndex:
    """Internal wrapper for HNSW index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$hnsw_index"
    VALUE_TYPE_NAME: Final[str] = FLOAT_LIST_VALUE_NAME

    def __init__(self, config: HnswIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalSpannIndex:
    """Internal wrapper for SPANN index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$spann_index"
    VALUE_TYPE_NAME: Final[str] = FLOAT_LIST_VALUE_NAME

    def __init__(self, config: SpannIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalVectorIndex:
    """Internal wrapper for vector index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$vector_index"
    VALUE_TYPE_NAME: Final[str] = FLOAT_LIST_VALUE_NAME

    def __init__(self, config: VectorIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalSparseVectorIndex:
    """Internal wrapper for sparse vector index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$sparse_vector_index"
    VALUE_TYPE_NAME: Final[str] = SPARSE_VECTOR_VALUE_NAME

    def __init__(self, config: SparseVectorIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalStringInvertedIndex:
    """Internal wrapper for string inverted index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$string_inverted_index"
    VALUE_TYPE_NAME: Final[str] = STRING_VALUE_NAME

    def __init__(self, config: StringInvertedIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalIntInvertedIndex:
    """Internal wrapper for int inverted index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$int_inverted_index"
    VALUE_TYPE_NAME: Final[str] = INT_VALUE_NAME

    def __init__(self, config: IntInvertedIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalFloatInvertedIndex:
    """Internal wrapper for float inverted index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$float_inverted_index"
    VALUE_TYPE_NAME: Final[str] = FLOAT_VALUE_NAME

    def __init__(self, config: FloatInvertedIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


class InternalBoolInvertedIndex:
    """Internal wrapper for bool inverted index with encapsulated name, value type, and enabled status."""
    NAME: Final[str] = "$bool_inverted_index"
    VALUE_TYPE_NAME: Final[str] = BOOL_VALUE_NAME

    def __init__(self, config: BoolInvertedIndexConfig, enabled: bool = True):
        self.config = config
        self.enabled = enabled


# Union type for all index configurations
IndexConfig = Union[
    FtsIndexConfig,
    VectorIndexConfig,
    SparseVectorIndexConfig,
    StringInvertedIndexConfig,
    IntInvertedIndexConfig,
    FloatInvertedIndexConfig,
    BoolInvertedIndexConfig,
]


# Type for index entry in schema
class IndexEntry(BaseModel):
    config: IndexConfig
    enabled: bool


def _get_class_name(config: IndexConfig) -> str:
    """Get the class name for a config."""
    # Pydantic models retain their class information at runtime
    return config.__class__.__name__


# Internal schema types with strong typing using existing Internal*Index types
# Union type for index values (boolean for default/unset, Internal*Index for configured)
InternalIndexType = Union[
    InternalFtsIndex,
    InternalVectorIndex,
    InternalSparseVectorIndex,
    InternalStringInvertedIndex,
    InternalIntInvertedIndex,
    InternalFloatInvertedIndex,
    InternalBoolInvertedIndex
]

# Type for a value type's index configuration (reused in multiple places)
ValueTypeIndexes = Dict[str, Union[bool, InternalIndexType]]


# Schema builder and final schema classes
class Schema:
    """Schema builder for collection index configurations."""

    def __init__(self) -> None:
        # Dict structure: {key: {index_type_name: IndexEntry}}
        self._index_configs: Dict[str, Dict[str, IndexEntry]] = {}
        # Dict structure: {index_type_name: IndexEntry}
        self._global_configs: Dict[str, IndexEntry] = {}

    def create_index(self, config: Optional[IndexConfig] = None, key: Optional[str] = None) -> "Schema":
        """Create an index configuration."""
        # Disallow config=None and key=None - too dangerous
        if config is None and key is None:
            raise ValueError("Cannot enable all index types globally. Must specify either config or key.")

        # Case 1: config is not None and key is None - enable specific index type globally
        if config is not None and key is None:
            # For TypedDict configs, we need to determine the type by checking the structure
            index_type_name = _get_class_name(config)
            self._global_configs[index_type_name] = IndexEntry(config=config, enabled=True)

        # Case 2: config is None and key is not None - enable all index types for that key
        elif config is None and key is not None:
            if key not in self._index_configs:
                self._index_configs[key] = {}
            for config_class in IndexConfig.__args__:  # type: ignore
                index_type_name = config_class.__name__
                default_config = config_class()
                self._index_configs[key][index_type_name] = IndexEntry(config=default_config, enabled=True)

        # Case 3: config is not None and key is not None - enable specific index type for that key
        elif config is not None and key is not None:
            index_type_name = _get_class_name(config)
            if key not in self._index_configs:
                self._index_configs[key] = {}
            self._index_configs[key][index_type_name] = IndexEntry(config=config, enabled=True)

        return self

    def delete_index(self, config: Optional[IndexConfig] = None, key: Optional[str] = None) -> "Schema":
        """Disable an index configuration (set enabled=False)."""
        # Case 1: Both config and key are None - fail the request
        if config is None and key is None:
            raise ValueError("Cannot disable all indexes. Must specify either config or key.")

        # Case 2: key is not None and config is None - disable all possible index types for that key
        if key is not None and config is None:
            if key not in self._index_configs:
                self._index_configs[key] = {}

            # Disable all possible index types for this key
            for config_class in IndexConfig.__args__:  # type: ignore
                index_type_name = config_class.__name__
                default_config = config_class()
                self._index_configs[key][index_type_name] = IndexEntry(config=default_config, enabled=False)

        # Case 3: key is not None and config is not None - disable specific index for that key
        elif key is not None and config is not None:
            index_type_name = _get_class_name(config)
            if key not in self._index_configs:
                self._index_configs[key] = {}
            self._index_configs[key][index_type_name] = IndexEntry(config=config, enabled=False)

        # Case 4: key is None and config is not None - disable specific index globally
        elif key is None and config is not None:
            index_type_name = _get_class_name(config)
            self._global_configs[index_type_name] = IndexEntry(config=config, enabled=False)

        return self


class InternalSchema(BaseModel):
    """Internal schema representation for server-side processing."""
    model_config = {"arbitrary_types_allowed": True}
    defaults: Dict[str, ValueTypeIndexes]
    key_overrides: Dict[str, Dict[str, ValueTypeIndexes]]

    # Index type mappings for deserialization and schema conversion
    # Maps index names to (Internal*Index class, Config class) tuples
    _INDEX_TYPE_MAP: ClassVar[Dict[str, Tuple[Any, Any]]] = {
        FTS_INDEX_NAME: (InternalFtsIndex, FtsIndexConfig),
        VECTOR_INDEX_NAME: (InternalVectorIndex, VectorIndexConfig),
        SPARSE_VECTOR_INDEX_NAME: (InternalSparseVectorIndex, SparseVectorIndexConfig),
        STRING_INVERTED_INDEX_NAME: (InternalStringInvertedIndex, StringInvertedIndexConfig),
        INT_INVERTED_INDEX_NAME: (InternalIntInvertedIndex, IntInvertedIndexConfig),
        FLOAT_INVERTED_INDEX_NAME: (InternalFloatInvertedIndex, FloatInvertedIndexConfig),
        BOOL_INVERTED_INDEX_NAME: (InternalBoolInvertedIndex, BoolInvertedIndexConfig),
        HNSW_INDEX_NAME: (InternalHnswIndex, HnswIndexConfig),
        SPANN_INDEX_NAME: (InternalSpannIndex, SpannIndexConfig),
    }

    # Maps config class names to their internal representations
    _CONFIG_TO_INTERNAL_MAP: ClassVar[Dict[str, Any]] = {
        'FtsIndexConfig': InternalFtsIndex,
        'VectorIndexConfig': InternalVectorIndex,
        'SparseVectorIndexConfig': InternalSparseVectorIndex,
        'StringInvertedIndexConfig': InternalStringInvertedIndex,
        'IntInvertedIndexConfig': InternalIntInvertedIndex,
        'FloatInvertedIndexConfig': InternalFloatInvertedIndex,
        'BoolInvertedIndexConfig': InternalBoolInvertedIndex,
    }

    # Maps value types to supported index types
    _VALUE_TYPE_TO_INDEX_TYPES: ClassVar[Dict[str, List[str]]] = {
        STRING_VALUE_NAME: [STRING_INVERTED_INDEX_NAME, FTS_INDEX_NAME],
        FLOAT_VALUE_NAME: [FLOAT_INVERTED_INDEX_NAME],
        FLOAT_LIST_VALUE_NAME: [VECTOR_INDEX_NAME],
        SPARSE_VECTOR_VALUE_NAME: [SPARSE_VECTOR_INDEX_NAME],
        BOOL_VALUE_NAME: [BOOL_INVERTED_INDEX_NAME],
        INT_VALUE_NAME: [INT_INVERTED_INDEX_NAME],
    }

    def _initialize_defaults(self, defaults: Dict[str, ValueTypeIndexes]) -> None:
        """Initialize defaults with base structure and standard configuration."""
        # Set all supported index types to enabled by default
        for value_type, index_types in self._VALUE_TYPE_TO_INDEX_TYPES.items():
            defaults[value_type] = {}
            for index_type in index_types:
                defaults[value_type][index_type] = True

        # Apply specific default overrides for certain index types
        # Most indexes are enabled by default, but some are disabled for performance reasons

        # "#sparse_vector": { "$sparse_vector_index": False }
        defaults[SPARSE_VECTOR_VALUE_NAME][SPARSE_VECTOR_INDEX_NAME] = False

        # For string values, prefer inverted index over full-text search for better performance
        defaults[STRING_VALUE_NAME][FTS_INDEX_NAME] = False

        # "#float_list": { "$vector_index": False }
        defaults[FLOAT_LIST_VALUE_NAME][VECTOR_INDEX_NAME] = False

    def _initialize_key_overrides(self, key_overrides: Dict[str, Dict[str, ValueTypeIndexes]]) -> None:
        """Initialize key-specific index overrides."""
        # Enable full-text search for document content
        key_overrides[DOCUMENT_KEY] = {
            STRING_VALUE_NAME: {
                FTS_INDEX_NAME: True,
                STRING_INVERTED_INDEX_NAME: False
            }
        }

        # Enable vector index for embeddings with document source reference
        vector_config = VectorIndexConfig(source_key=DOCUMENT_KEY)
        key_overrides[EMBEDDING_KEY] = {
            FLOAT_LIST_VALUE_NAME: {
                VECTOR_INDEX_NAME: InternalVectorIndex(
                    config=vector_config,
                    enabled=True
                )
            }
        }

    def __init__(self, schema: Schema) -> None:
        """Create InternalSchema from a client-facing Schema."""
        defaults: Dict[str, ValueTypeIndexes] = {}
        key_overrides: Dict[str, Dict[str, ValueTypeIndexes]] = {}

        # Initialize with standard defaults
        self._initialize_defaults(defaults)
        self._initialize_key_overrides(key_overrides)

        # Process global configs
        for config_type_name, index_entry in schema._global_configs.items():
            internal_class = self._CONFIG_TO_INTERNAL_MAP.get(config_type_name)
            if internal_class:
                value_type = internal_class.VALUE_TYPE_NAME
                index_name = internal_class.NAME

                # Apply user-specified global configurations
                defaults[value_type][index_name] = internal_class(
                    config=index_entry.config,
                    enabled=index_entry.enabled
                )

        # Process key-specific configs
        for key, key_configs in schema._index_configs.items():
            # Initialize key if not already present
            if key not in key_overrides:
                key_overrides[key] = {}

            for config_type_name, index_entry in key_configs.items():
                internal_class = self._CONFIG_TO_INTERNAL_MAP.get(config_type_name)
                if internal_class:
                    value_type = internal_class.VALUE_TYPE_NAME
                    index_name = internal_class.NAME

                    # Create value_type dict only when we have configs for it
                    if value_type not in key_overrides[key]:
                        key_overrides[key][value_type] = {}

                    # Apply user-specified key configurations
                    key_overrides[key][value_type][index_name] = internal_class(
                        config=index_entry.config,
                        enabled=index_entry.enabled
                    )

        # Initialize the Pydantic model with computed values
        super().__init__(defaults=defaults, key_overrides=key_overrides)

    def _serialize_value_type_indexes(self, value_type_indexes: ValueTypeIndexes) -> Dict[str, Any]:
        """Convert a ValueTypeIndexes dict to JSON-serializable format."""
        result: Dict[str, Any] = {}
        for index_name, index_value in value_type_indexes.items():
            if isinstance(index_value, bool):
                result[index_name] = index_value
            else:
                # Exclude None values from serialization
                config_dict = index_value.config.model_dump(exclude_none=True) if hasattr(index_value.config, 'model_dump') else index_value.config.__dict__
                result[index_name] = {
                    "enabled": index_value.enabled,
                    "config": config_dict
                }
        return result

    def serialize_to_json(self) -> Dict[str, Any]:
        """Convert InternalSchema to a JSON-serializable dict for transmission over the wire."""
        # Convert defaults to JSON format
        defaults_json = {}
        for value_type, indexes in self.defaults.items():
            defaults_json[value_type] = self._serialize_value_type_indexes(indexes)

        # Convert key overrides to JSON format
        key_overrides_json: Dict[str, Dict[str, Any]] = {}
        for key, value_types in self.key_overrides.items():
            key_overrides_json[key] = {}
            for value_type, indexes in value_types.items():
                key_overrides_json[key][value_type] = self._serialize_value_type_indexes(indexes)

        return {
            "defaults": defaults_json,
            "key_overrides": key_overrides_json
        }

    @classmethod
    def deserialize_from_json(cls, json_data: Dict[str, Any]) -> "InternalSchema":
        """Create InternalSchema from JSON-serialized data."""
        # Extract and deserialize components
        defaults = cls._deserialize_defaults(json_data.get("defaults", {}))
        key_overrides = cls._deserialize_key_overrides(json_data.get("key_overrides", {}))

        # Create instance directly from deserialized data
        instance = cls.model_construct(defaults=defaults, key_overrides=key_overrides)

        return instance

    @classmethod
    def _deserialize_defaults(cls, defaults_json: Dict[str, Any]) -> Dict[str, ValueTypeIndexes]:
        """Deserialize defaults from JSON format."""
        defaults: Dict[str, ValueTypeIndexes] = {}

        for value_type, indexes_json in defaults_json.items():
            defaults[value_type] = cls._deserialize_value_type_indexes(indexes_json)

        return defaults

    @classmethod
    def _deserialize_key_overrides(cls, key_overrides_json: Dict[str, Any]) -> Dict[str, Dict[str, ValueTypeIndexes]]:
        """Deserialize key_overrides from JSON format."""
        key_overrides: Dict[str, Dict[str, ValueTypeIndexes]] = {}

        for key, value_types_json in key_overrides_json.items():
            key_overrides[key] = {}
            for value_type, indexes_json in value_types_json.items():
                key_overrides[key][value_type] = cls._deserialize_value_type_indexes(indexes_json)

        return key_overrides

    @classmethod
    def _deserialize_value_type_indexes(cls, indexes_json: Dict[str, Any]) -> ValueTypeIndexes:
        """Deserialize ValueTypeIndexes from JSON format."""
        result: ValueTypeIndexes = {}

        for index_name, index_data in indexes_json.items():
            if isinstance(index_data, bool):
                result[index_name] = index_data
            else:
                # Reconstruct Internal*Index object
                index_mapping = cls._INDEX_TYPE_MAP.get(index_name)
                if index_mapping:
                    internal_class, config_class = index_mapping
                    config_obj = config_class(**index_data["config"])
                    result[index_name] = internal_class(config=config_obj, enabled=index_data["enabled"])
                else:
                    # Unknown index type - cannot reconstruct
                    raise ValueError(f"Unknown index type '{index_name}' during deserialization. Cannot reconstruct Internal*Index object.")

        return result
