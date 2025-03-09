from typing import Optional, Union, TypeVar, List, Dict, Any, cast
from numpy.typing import NDArray
import numpy as np
from typing_extensions import TypedDict, Protocol, runtime_checkable
from enum import Enum
from pydantic import Field
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
from tenacity import retry
from abc import abstractmethod


# Re-export types from chromadb.types
__all__ = [
    "Metadata",
    "Where",
    "WhereDocument",
    "UpdateCollectionMetadata",
    "UpdateMetadata",
]
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


class Space(Enum):
    COSINE = "cosine"
    L2 = "l2"
    INNER_PRODUCT = "inner_product"


def normalize_embeddings(
    target: Optional[Union[OneOrMany[Embedding], OneOrMany[PyEmbedding]]]
) -> Optional[Embeddings]:
    if target is None:
        return None

    if len(target) == 0:
        raise ValueError(
            f"Expected Embeddings to be non-empty list or numpy array, got {target}"
        )

    if isinstance(target, list):
        # One PyEmbedding
        if isinstance(target[0], (int, float)) and not isinstance(target[0], bool):
            return [np.array(target, dtype=np.float32)]
        # List of PyEmbeddings
        if isinstance(target[0], list):
            if isinstance(target[0][0], (int, float)) and not isinstance(
                target[0][0], bool
            ):
                return [np.array(embedding, dtype=np.float32) for embedding in target]
        # List of np.ndarrays
        if isinstance(target[0], np.ndarray):
            return cast(Embeddings, target)

    elif isinstance(target, np.ndarray):
        # A single embedding as a numpy array
        if target.ndim == 1:
            return cast(Embeddings, [target])
        # 2-D numpy array (comes out of embedding models)
        # TODO: Enforce this at the embedding function level
        if target.ndim == 2:
            return list(target)

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

    # TODO: Validate URIs


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

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: EmbeddingFunction[D], input: D) -> Embeddings:
            result = call(self, input)
            assert result is not None
            return cast(Embeddings, normalize_embeddings(result))

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
        raise NotImplementedError(
            "name() is not implemented for this embedding function."
        )

    def default_space(self) -> Space:
        """
        Return the default space for the embedding function.
        """
        return Space.COSINE

    def supported_spaces(self) -> List[Space]:
        """
        Return the supported spaces for the embedding function.
        """
        return [Space.COSINE, Space.L2, Space.INNER_PRODUCT]

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
        raise NotImplementedError(
            "build_from_config() is not implemented for this embedding function."
        )

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
        raise NotImplementedError(
            "get_config() is not implemented for this embedding function."
        )

    def validate_config_update(
        self, old_config: Dict[str, Any], new_config: Dict[str, Any]
    ) -> None:
        """
        Validate the update to the config.
        """
        return

    def validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate the config.
        """
        return


class DataLoader(Protocol[L]):
    def __call__(self, uris: URIs) -> L:
        ...


def convert_np_embeddings_to_list(embeddings: Embeddings) -> PyEmbeddings:
    # Cast the result to PyEmbeddings to ensure type compatibility
    return cast(PyEmbeddings, [embedding.tolist() for embedding in embeddings])


def convert_list_embeddings_to_np(embeddings: PyEmbeddings) -> Embeddings:
    return [np.array(embedding) for embedding in embeddings]
