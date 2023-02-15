from typing import Literal, Union, Dict, Sequence, TypedDict, Protocol, TypeVar, List

ID = str
IDs = List[ID]

Embedding = List[float]
Embeddings = List[Embedding]

Metadata = Dict[str, Union[str, int, float]]
Metadatas = List[Metadata]

Document = str
Documents = List[Document]

Parameter = TypeVar("Parameter", Embedding, Document, Metadata, ID)
T = TypeVar("T")
OneOrMany = Union[T, List[T]]


# Grammar for where expressions
LiteralValue = Union[str, int, float]
LogicalOperator = Literal["$and", "$or"]
WhereOperator = Literal["$gt", "$gte", "$lt", "$lte", "$ne", "$eq"]
OperatorExpression = Dict[Union[WhereOperator, LogicalOperator], LiteralValue]

Where = Dict[Union[str, LogicalOperator], Union[LiteralValue, OperatorExpression, List["Where"]]]

WhereDocumentOperator = Literal["$contains", LogicalOperator]
WhereDocument = Dict[WhereDocumentOperator, Union[str, List["WhereDocument"]]]


class GetResult(TypedDict):
    ids: List[ID]
    embeddings: List[Embedding]
    documents: List[Document]
    metadatas: List[Metadata]


class QueryResult(TypedDict):
    ids: List[IDs]
    embeddings: List[List[Embedding]]
    documents: List[List[Document]]
    metadatas: List[List[Metadata]]
    distances: List[List[float]]


class EmbeddingFunction(Protocol):
    def __call__(self, texts: Documents) -> Embeddings:
        ...


def maybe_cast_one_to_many(
    target: OneOrMany[Parameter],
) -> List[Parameter]:
    """Infers if target is Embedding, Metadata, or Document and casts it to a many object if its one"""

    if isinstance(target, Sequence):
        # One Document or ID
        if isinstance(target, str) and target != None:
            return [target]  # type: ignore
        # One Embedding
        if isinstance(target[0], float):
            return [target]  # type: ignore
    # One Metadata dict
    if isinstance(target, dict):
        return [target]
    # Already a sequence
    return target  # type: ignore


def validate_metadata(metadata: Metadata) -> Metadata:
    """Validates metadata to ensure it is a dictionary of strings to strings, ints, or floats"""
    if not isinstance(metadata, dict):
        raise ValueError("Metadata must be a dictionary")
    for key, value in metadata.items():
        if not isinstance(key, str):
            raise ValueError(f"Metadata key {key} must be a string")
        if not isinstance(value, (str, int, float)):
            raise ValueError(f"Metadata value {value} must be a string, int, or float")
    return metadata


def validate_metadatas(metadatas: Metadatas) -> Metadatas:
    """Validates metadatas to ensure it is a list of dictionaries of strings to strings, ints, or floats"""
    if not isinstance(metadatas, list):
        raise ValueError("Metadatas must be a list")
    for metadata in metadatas:
        validate_metadata(metadata)
    return metadatas


def validate_where(where: Where) -> Where:
    """
    Validates where to ensure it is a dictionary of strings to strings, ints, floats or operator expressions,
    or in the case of $and and $or, a list of where expressions
    """
    if not isinstance(where, dict):
        raise ValueError("Where must be a dictionary")
    for key, value in where.items():
        if not isinstance(key, str):
            raise ValueError(f"Where key {key} must be a string")
        if key != "$and" and key != "$or" and not isinstance(value, (str, int, float, dict)):
            raise ValueError(
                f"Where value {value} must be a string, int, or float, or operator expression"
            )
        if key == "$and" or key == "$or":
            if not isinstance(value, list):
                raise ValueError(
                    f"Where value {value} for $and or $or must be a list of where expressions"
                )
            if len(value) <= 1:
                raise ValueError(
                    f"Where value {value} for $and or $or must have at least two where expressions"
                )
            for where_expression in value:
                validate_where(where_expression)
        # Value is a operator expression
        if isinstance(value, dict):
            # Ensure there is only one operator
            if len(value) != 1:
                raise ValueError(
                    f"Where operator expression {value} must have exactly one operator"
                )

            for operator, operand in value.items():
                # Only numbers can be compared with gt, gte, lt, lte
                if operator in ["$gt", "$gte", "$lt", "$lte"]:
                    if not isinstance(operand, (int, float)):
                        raise ValueError(
                            f"Where operand value {operand} must be an int or float for operator {operator}"
                        )

                if operator not in ["$gt", "$gte", "$lt", "$lte", "$ne", "$eq"]:
                    raise ValueError(
                        f"Where operator must be one of $gt, $gte, $lt, $lte, $ne", "$eq"
                    )

                if not isinstance(operand, (str, int, float)):
                    raise ValueError(
                        f"Where operand value {operand} must be a string, int, or float"
                    )
    return where


def validate_where_document(where_document: WhereDocument) -> WhereDocument:
    """
    Validates where_document to ensure it is a dictionary of WhereDocumentOperator to strings, or in the case of $and and $or,
    a list of where_document expressions
    """
    if not isinstance(where_document, dict):
        raise ValueError("Where document must be a dictionary")
    if len(where_document) != 1:
        raise ValueError("Where document must have exactly one operator")
    for operator, operand in where_document.items():
        if operator not in ["$contains", "$and", "$or"]:
            raise ValueError(f"Where document operator must be $contains, $and, or $or")
        if operator == "$and" or operator == "$or":
            if not isinstance(operand, list):
                raise ValueError(
                    f"Where document value {operand} for $and or $or must be a list of where document expressions"
                )
            if len(operand) <= 1:
                raise ValueError(
                    f"Where document value {operand} for $and or $or must have at least two where document expressions"
                )
            for where_document_expression in operand:
                validate_where_document(where_document_expression)
        # Value is a $contains operator
        elif not isinstance(operand, str):
            raise ValueError(
                f"Where document operand value {operand} must be a string for operator $contains"
            )
    return where_document
