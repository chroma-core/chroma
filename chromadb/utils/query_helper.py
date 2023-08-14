from typing import List, Union, Literal, Any, Dict, cast

from chromadb.types import (
    WhereOperator,
    LiteralValue,
    WhereDocument,
    Where,
    LogicalOperator,
)


class WhereFilters(Where):
    """
    A builder class for creating a where clause for a filter.
    """

    def _add_condition(
        self,
        field: str,
        operator: Union[WhereOperator, Literal["$in"]],
        value: Union[LiteralValue, List[LiteralValue]],
    ) -> "Where":
        """
        Add a condition to the filter query.
        """
        if field not in self.keys():
            self[field] = {}
        self[field][operator] = value
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

    def and_(self, *conditions: "Where") -> "Where":
        """
        Add an and condition to the filter query.
        """
        if "$and" not in self.keys():
            self["$and"] = []
        for condition in conditions:
            self["$and"].append(condition)
        return self

    def or_(self, *conditions: "Where") -> "Where":
        """
        Add an or condition to the filter query.
        """
        if "$or" not in self.keys():
            self["$or"] = []
        for condition in conditions:
            self["$or"].append(condition)
        return self

    def to_filter(self) -> Dict[str, Any]:
        """
        Return the filter query.
        """
        return self


class WhereDocumentFilter(WhereDocument):
    """
    A builder class for creating a where clause for a filter.
    """

    def contains(self, value: str) -> "WhereDocumentFilter":
        """
        Add a contains condition to the filter query.
        """
        self["$contains"] = value
        return self

    def and_(self, *conditions: "WhereDocumentFilter") -> "WhereDocumentFilter":
        """
        Add an and condition to the filter query.
        """
        self["$and"] = [condition for condition in conditions]
        return self

    def or_(self, *conditions: "WhereDocumentFilter") -> "WhereDocumentFilter":
        """
        Add an or condition to the filter query.
        """
        self["$or"] = [condition for condition in conditions]
        return self


class Filters(object):
    """Global Filter class"""

    @staticmethod
    def where() -> "WhereFilters":
        """
        Return a new WhereFilters instance.
        """
        return WhereFilters()

    @staticmethod
    def where_document() -> "WhereDocumentFilter":
        """
        Return a new WhereDocumentFilter instance.
        """
        return WhereDocumentFilter()


class AttrGroup(Where):
    def __init__(
        self,
        lhs: Union[LiteralValue, "AttrGroup"],
        operator: Union[WhereOperator, LogicalOperator, Literal["$not"]],
        rhs: Union[List[LiteralValue], LiteralValue, "AttrGroup"],
    ):
        # self.lhs = lhs
        # self.operator = operator
        # self.rhs = rhs
        print(f"addd: {lhs} {operator} {rhs}")
        if operator in ["$and", "$or"]:
            self[operator] = [lhs, rhs]
        elif operator == "$in":
            self[operator] = {lhs: rhs}
        elif (
            operator == "$not"
            and isinstance(lhs, type(self))
            and list(lhs.keys())[0] == "$in"
        ):
            print("-----")
            self["$nin"] = lhs["$in"]
        else:
            self[lhs] = {operator: rhs}

    def __and__(self, other: "AttrGroup") -> "AttrGroup":
        print("dqwweqw")
        return AttrGroup(self, "$and", other)

    def __or__(self, other: "AttrGroup") -> "AttrGroup":
        return AttrGroup(self, "$or", other)

    def __invert__(self) -> "AttrGroup":
        return AttrGroup(self, "$not", None)


class Attr:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other: LiteralValue) -> AttrGroup:
        print("EQ")
        return AttrGroup(self.name, "$eq", other)

    def __str__(self) -> str:
        return self.name

    def __and__(self, other):
        print("dwqweqw")

    def __rshift__(self, other):
        return AttrGroup(self.name, "$in", other)


if __name__ == "__main__":
    print((Attr("a") == 1) | ((Attr("category") == 10) & (Attr("price") == 100)))
    print((Attr("category") == 10) & (Attr("price") == 100))
    print((Attr("category") == 10))
    print((Attr("category") >> ["business", "sports"]))
    print(~(Attr("category") >> ["business", "sports"]))
