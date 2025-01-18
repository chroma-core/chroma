from inspect import signature
from typing import Any, Dict, Optional, Set, Tuple, Union

import numpy as np
from chromadb.api.types import (
    BaseRecordSet,
    Documents,
    EmbeddingFunction,
    Embeddings,
    FilterSet,
    IDs,
    Images,
    Include,
    IncludeEnum,
    InsertRecordSet,
    Metadatas,
    PyEmbeddings,
    URIs,
    is_document,
    is_image,
    Embeddable,
    Metadata,
    UpdateMetadata,
    Where,
    WhereDocument,
)
import chromadb.errors as errors


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


def validate_metadatas(metadatas: Metadatas) -> Metadatas:
    """Validates metadatas to ensure it is a list of dictionaries of strings to strings, ints, floats or bools"""
    if not isinstance(metadatas, list):
        raise ValueError(f"Expected metadatas to be a list, got {metadatas}")
    for metadata in metadatas:
        validate_metadata(metadata)
    return metadatas


def validate_insert_record_set(record_set: InsertRecordSet) -> None:
    """
    Validates the InsertRecordSet, ensuring that all fields are of the right type and length.
    """
    _validate_record_set_length_consistency(record_set)
    validate_base_record_set(record_set)

    validate_ids(record_set["ids"])
    if record_set["metadatas"] is not None:
        validate_metadatas(record_set["metadatas"])


def get_default_embeddable_record_set_fields() -> Set[str]:
    """
    Returns the set of fields that can be embedded on a Record Set.
    This is a way to avoid hardcoding the fields in multiple places,
    and keeps them immutable.
    """
    return {"documents", "images", "uris"}


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


def validate_filter_set(filter_set: FilterSet) -> None:
    if filter_set["where"] is not None:
        validate_where(filter_set["where"])
    if filter_set["where_document"] is not None:
        validate_where_document(filter_set["where_document"])


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


META_KEY_CHROMA_DOCUMENT = "chroma:document"


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


def validate_include(include: Include, dissalowed: Optional[Include] = None) -> None:
    """Validates include to ensure it is a list of strings. Since get does not allow distances, allow_distances is used
    to control if distances is allowed"""

    if not isinstance(include, list):
        raise ValueError(f"Expected include to be a list, got {include}")
    for item in include:
        if not isinstance(item, str):
            raise ValueError(f"Expected include item to be a str, got {item}")

        if not any(item == e for e in IncludeEnum):
            raise ValueError(
                f"Expected include item to be one of {', '.join(IncludeEnum)}, got {item}"
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
