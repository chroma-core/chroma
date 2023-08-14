from typing import List, Union, Literal, Any, Dict, cast

from chromadb.types import (
    WhereOperator,
    LiteralValue,
    WhereDocument as WhereDocumentType,
)


class Where(object):
    """
    A builder class for creating a where clause for a filter.
    """

    query: Dict[str, Any]

    def __init__(self) -> None:
        self.query = {}

    def _add_condition(
        self,
        field: str,
        operator: Union[WhereOperator, Literal["$in"]],
        value: Union[LiteralValue, List[LiteralValue]],
    ) -> "Where":
        """
        Add a condition to the filter query.
        """
        if field not in self.query:
            self.query[field] = {}
        self.query[field][operator] = value
        return self

    def gt(self, field: str, value: LiteralValue) -> "Where":
        """
        Add a greater than condition to the filter query.
        """
        return self._add_condition(field, "$gt", value)

    def gte(self, field: str, value: LiteralValue) -> "Where":
        """
        Add a greater than or equal to condition to the filter query.
        """
        return self._add_condition(field, "$gte", value)

    def lt(self, field: str, value: LiteralValue) -> "Where":
        """
        Add a less than condition to the filter query.
        """
        return self._add_condition(field, "$lt", value)

    def lte(self, field: str, value: LiteralValue) -> "Where":
        """
        Add a less than or equal to condition to the filter query.
        """
        return self._add_condition(field, "$lte", value)

    def ne(self, field: str, value: LiteralValue) -> "Where":
        """
        Add a not equal to condition to the filter query.
        """
        return self._add_condition(field, "$ne", value)

    def eq(self, field: str, value: LiteralValue) -> "Where":
        """
        Add an equal to condition to the filter query.
        """
        return self._add_condition(field, "$eq", value)

    # def in_(self, field: str, values: List[LiteralValue]) -> "Where":
    #     """
    #     Add an in condition to the filter query.
    #     """
    #     return self._add_condition(field, "$in", values)

    def and_(self, *conditions: "Where") -> "Where":
        """
        Add an and condition to the filter query.
        """
        if "$and" not in self.query:
            self.query["$and"] = []
        for condition in conditions:
            self.query["$and"].append(condition.query)
        return self

    def or_(self, *conditions: "Where") -> "Where":
        """
        Add an or condition to the filter query.
        """
        if "$or" not in self.query:
            self.query["$or"] = []
        for condition in conditions:
            self.query["$or"].append(condition.query)
        return self

    def to_filter(self) -> Dict[str, Any]:
        """
        Return the filter query.
        """
        return self.query


class WhereDocument(object):
    """
    A builder class for creating a where clause for a filter.
    """

    query: Dict[str, Any]

    def __init__(self) -> None:
        self.query = {}

    def contains(self, value: str) -> "WhereDocument":
        """
        Add a contains condition to the filter query.
        """
        self.query["$contains"] = value
        return self

    def and_(self, *conditions: "WhereDocument") -> "WhereDocument":
        """
        Add an and condition to the filter query.
        """
        self.query["$and"] = [condition.query for condition in conditions]
        return self

    def or_(self, *conditions: "WhereDocument") -> "WhereDocument":
        """
        Add an or condition to the filter query.
        """
        self.query["$or"] = [condition.query for condition in conditions]
        return self

    def to_filter(self) -> WhereDocumentType:
        return cast(WhereDocumentType, self.query)
