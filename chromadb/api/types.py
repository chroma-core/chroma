from typing import (
    Optional,
    Type,
    Union,
    TypeVar,
    List,
    Dict,
    Any,
    Tuple,
    cast,
)
from numpy.typing import NDArray
import numpy as np
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
from inspect import signature
from tenacity import retry
from chromadb.utils.the_registry import _register
import json

# Re-export types from chromadb.types
__all__ = ["Metadata", "Where", "WhereDocument", "UpdateCollectionMetadata"]
META_KEY_CHROMA_DOCUMENT = "chroma:document"
T = TypeVar("T")
OneOrMany = Union[T, List[T]]

# URIs
URI = str
URIs = List[URI]


def maybe_cast_one_to_many_uri(target: OneOrMany[URI]) -> URIs:
    if isinstance(target, str):
        # One URI
        return cast(URIs, [target])
    # Already a sequence
    return cast(URIs, target)


# IDs
ID = str
IDs = List[ID]


def maybe_cast_one_to_many_ids(target: OneOrMany[ID]) -> IDs:
    if isinstance(target, str):
        # One ID
        return cast(IDs, [target])
    # Already a sequence
    return cast(IDs, target)


# Embeddings
Embedding = Vector
Embeddings = List[Embedding]


def maybe_cast_one_to_many_embedding(target: OneOrMany[Embedding]) -> Embeddings:
    if isinstance(target, List):
        # One Embedding
        if isinstance(target[0], (int, float)):
            return cast(Embeddings, [target])
    # Already a sequence
    return cast(Embeddings, target)


# Metadatas
Metadatas = List[Metadata]


def maybe_cast_one_to_many_metadata(target: OneOrMany[Metadata]) -> Metadatas:
    # One Metadata dict
    if isinstance(target, dict):
        return cast(Metadatas, [target])
    # Already a sequence
    return cast(Metadatas, target)


CollectionMetadata = Dict[str, Any]
UpdateCollectionMetadata = UpdateMetadata

# Documents
Document = str
Documents = List[Document]


def is_document(target: Any) -> bool:
    if not isinstance(target, str):
        return False
    return True


def maybe_cast_one_to_many_document(target: OneOrMany[Document]) -> Documents:
    # One Document
    if is_document(target):
        return cast(Documents, [target])
    # Already a sequence
    return cast(Documents, target)


# Images
ImageDType = Union[np.uint, np.int_, np.float_]
Image = NDArray[ImageDType]
Images = List[Image]


def is_image(target: Any) -> bool:
    if not isinstance(target, np.ndarray):
        return False
    if len(target.shape) < 2:
        return False
    return True


def maybe_cast_one_to_many_image(target: OneOrMany[Image]) -> Images:
    if is_image(target):
        return cast(Images, [target])
    # Already a sequence
    return cast(Images, target)


Parameter = TypeVar("Parameter", Document, Image, Embedding, Metadata, ID)

# This should ust be List[Literal["documents", "embeddings", "metadatas", "distances"]]
# However, this provokes an incompatibility with the Overrides library and Python 3.7
Include = List[
    Union[
        Literal["documents"],
        Literal["embeddings"],
        Literal["metadatas"],
        Literal["distances"],
        Literal["uris"],
        Literal["data"],
    ]
]

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
    embeddings: Optional[List[Embedding]]
    documents: Optional[List[Document]]
    uris: Optional[URIs]
    data: Optional[Loadable]
    metadatas: Optional[List[Metadata]]
    included: Include


class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: Optional[List[List[Embedding]]]
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


# region: They told me I was mad. Well who's mad now?!


class StoreInitArgsMeta(type):
    @staticmethod
    def _serialize_args_kwargs(*args: Any, **kwargs: Any) -> str:
        args_list = list(args)
        try:
            serialized = json.dumps({"args": args_list, "kwargs": kwargs})
        except Exception as e:
            raise ValueError(f"Failed to serialize args and kwargs: {e}")
        return serialized

    @staticmethod
    def _deserialize_args_kwargs(
        serialized: str,
    ) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
        try:
            deserialized = json.loads(serialized)
            return tuple(deserialized["args"]), deserialized["kwargs"]
        except Exception as e:
            raise ValueError(f"Failed to deserialize args and kwargs: {e}")

    def __call__(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        instance: T = super().__call__(*args, **kwargs)  # type: ignore[misc]
        setattr(
            instance,
            "_init_args",
            StoreInitArgsMeta._serialize_args_kwargs(*args, **kwargs),
        )
        return instance

    def from_init_args(cls: Type[T], init_args: str) -> T:
        args, kwargs = StoreInitArgsMeta._deserialize_args_kwargs(init_args)
        return cls(*args, **kwargs)


Ty = TypeVar("Ty", bound=type)


class RegisterMeta(type):
    def __new__(
        cls: Type[Ty], name: str, bases: Tuple[type, ...], dct: Dict[str, Any]
    ) -> Ty:
        # Create the new class using super()
        new_class: Ty = super().__new__(cls, name, bases, dct)  # type: ignore[misc]
        # Register the class automatically if it is a direct or indirect subclass
        if bases:  # This condition avoids registering the base class itself
            _register(new_class)
        return new_class


class StoreProtocolMeta(StoreInitArgsMeta, type(Protocol)):  # type: ignore[misc]
    pass


class RegisterProtocolMeta(RegisterMeta, type(Protocol)):  # type: ignore[misc]
    pass


class StoreAndRegisterProtocolMeta(
    StoreProtocolMeta, RegisterProtocolMeta, type(Protocol)  # type: ignore[misc]
):
    pass


class EmbeddingFunction(Protocol[D], metaclass=StoreAndRegisterProtocolMeta):
    def __call__(self, input: D) -> Embeddings:
        ...

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        # Raise an exception if __call__ is not defined since it is expected to be defined
        call = getattr(cls, "__call__")

        def __call__(self: EmbeddingFunction[D], input: D) -> Embeddings:
            result = call(self, input)
            return validate_embeddings(maybe_cast_one_to_many_embedding(result))

        setattr(cls, "__call__", __call__)

    def embed_with_retries(self, input: D, **retry_kwargs: Dict) -> Embeddings:  # type: ignore
        return retry(**retry_kwargs)(self.__call__)(input)  # type: ignore


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
    """Validates embeddings to ensure it is a list of list of ints, or floats"""
    if not isinstance(embeddings, list):
        raise ValueError(
            f"Expected embeddings to be a list, got {type(embeddings).__name__}"
        )
    if len(embeddings) == 0:
        raise ValueError(
            f"Expected embeddings to be a list with at least one item, got {len(embeddings)} embeddings"
        )
    if not all([isinstance(e, list) for e in embeddings]):
        raise ValueError(
            "Expected each embedding in the embeddings to be a list, got "
            f"{list(set([type(e).__name__ for e in embeddings]))}"
        )
    for i, embedding in enumerate(embeddings):
        if len(embedding) == 0:
            raise ValueError(
                f"Expected each embedding in the embeddings to be a non-empty list, got empty embedding at pos {i}"
            )
        if not all(
            [
                isinstance(value, (int, float)) and not isinstance(value, bool)
                for value in embedding
            ]
        ):
            raise ValueError(
                "Expected each value in the embedding to be a int or float, got an embedding with "
                f"{list(set([type(value).__name__ for value in embedding]))} - {embedding}"
            )
    return embeddings


def validate_batch(
    batch: Tuple[
        IDs,
        Optional[Embeddings],
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
