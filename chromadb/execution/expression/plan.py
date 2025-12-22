from dataclasses import dataclass, field
from typing import List, Dict, Any, Union, Set, Optional

from chromadb.execution.expression.operator import (
    KNN,
    Filter,
    GroupBy,
    Limit,
    Projection,
    Scan,
    Rank,
    Select,
    Where,
    Key,
)


@dataclass
class CountPlan:
    scan: Scan


@dataclass
class GetPlan:
    scan: Scan
    filter: Filter = field(default_factory=Filter)
    limit: Limit = field(default_factory=Limit)
    projection: Projection = field(default_factory=Projection)


@dataclass
class KNNPlan:
    scan: Scan
    knn: KNN
    filter: Filter = field(default_factory=Filter)
    projection: Projection = field(default_factory=Projection)


class Search:
    """Payload for hybrid search operations.

    Can be constructed directly or using builder pattern:

    Direct construction with expressions:
        Search(
            where=Key("status") == "active",
            rank=Knn(query=[0.1, 0.2]),
            limit=Limit(limit=10),
            select=Select(keys={Key.DOCUMENT})
        )

    Direct construction with dicts:
        Search(
            where={"status": "active"},
            rank={"$knn": {"query": [0.1, 0.2]}},
            limit=10,  # Creates Limit(limit=10, offset=0)
            select=["#document", "#score"]
        )

    Builder pattern:
        (Search()
            .where(Key("status") == "active")
            .rank(Knn(query=[0.1, 0.2]))
            .limit(10)
            .select(Key.DOCUMENT))

    Builder pattern with dicts:
        (Search()
            .where({"status": "active"})
            .rank({"$knn": {"query": [0.1, 0.2]}})
            .limit(10)
            .select(Key.DOCUMENT))

    Filter by IDs:
        Search().where(Key.ID.is_in(["id1", "id2", "id3"]))

    Combined with metadata filtering:
        Search().where((Key.ID.is_in(["id1", "id2"])) & (Key("status") == "active"))

    With group_by:
        (Search()
            .rank(Knn(query=[0.1, 0.2]))
            .group_by(GroupBy(
                keys=[Key("category")],
                aggregate=MinK(keys=[Key.SCORE], k=3)
            )))

    Empty Search() is valid and will use defaults:
        - where: None (no filtering)
        - rank: None (no ranking - results ordered by default order)
        - group_by: None (no grouping)
        - limit: No limit
        - select: Empty selection
    """

    def __init__(
        self,
        where: Optional[Union[Where, Dict[str, Any]]] = None,
        rank: Optional[Union[Rank, Dict[str, Any]]] = None,
        group_by: Optional[Union[GroupBy, Dict[str, Any]]] = None,
        limit: Optional[Union[Limit, Dict[str, Any], int]] = None,
        select: Optional[Union[Select, Dict[str, Any], List[str], Set[str]]] = None,
    ):
        """Initialize a Search with optional parameters.

        Args:
            where: Where expression or dict for filtering results (defaults to None - no filtering)
                   Dict will be converted using Where.from_dict()
            rank: Rank expression or dict for scoring (defaults to None - no ranking)
                  Dict will be converted using Rank.from_dict()
                  Note: Primitive numbers are not accepted - use {"$val": number} for constant ranks
            group_by: GroupBy configuration for grouping and aggregating results (defaults to None)
                      Dict will be converted using GroupBy.from_dict()
            limit: Limit configuration for pagination (defaults to no limit)
                   Can be a Limit object, a dict for Limit.from_dict(), or an int
                   When passing an int, it creates Limit(limit=value, offset=0)
            select: Select configuration for keys (defaults to empty selection)
                    Can be a Select object, a dict for Select.from_dict(),
                    or a list/set of strings (e.g., ["#document", "#score"])
        """
        # Handle where parameter
        if where is None:
            self._where = None
        elif isinstance(where, Where):
            self._where = where
        elif isinstance(where, dict):
            self._where = Where.from_dict(where)
        else:
            raise TypeError(
                f"where must be a Where object, dict, or None, got {type(where).__name__}"
            )

        # Handle rank parameter
        if rank is None:
            self._rank = None
        elif isinstance(rank, Rank):
            self._rank = rank
        elif isinstance(rank, dict):
            self._rank = Rank.from_dict(rank)
        else:
            raise TypeError(
                f"rank must be a Rank object, dict, or None, got {type(rank).__name__}"
            )

        # Handle group_by parameter
        if group_by is None:
            self._group_by = GroupBy()
        elif isinstance(group_by, GroupBy):
            self._group_by = group_by
        elif isinstance(group_by, dict):
            self._group_by = GroupBy.from_dict(group_by)
        else:
            raise TypeError(
                f"group_by must be a GroupBy object, dict, or None, got {type(group_by).__name__}"
            )

        # Handle limit parameter
        if limit is None:
            self._limit = Limit()
        elif isinstance(limit, Limit):
            self._limit = limit
        elif isinstance(limit, int):
            self._limit = Limit.from_dict({"limit": limit, "offset": 0})
        elif isinstance(limit, dict):
            self._limit = Limit.from_dict(limit)
        else:
            raise TypeError(
                f"limit must be a Limit object, dict, int, or None, got {type(limit).__name__}"
            )

        # Handle select parameter
        if select is None:
            self._select = Select()
        elif isinstance(select, Select):
            self._select = select
        elif isinstance(select, dict):
            self._select = Select.from_dict(select)
        elif isinstance(select, (list, set)):
            # Convert list/set of strings to Select object
            self._select = Select.from_dict({"keys": list(select)})
        else:
            raise TypeError(
                f"select must be a Select object, dict, list, set, or None, got {type(select).__name__}"
            )

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Search to a dictionary for JSON serialization"""
        return {
            "filter": self._where.to_dict() if self._where is not None else None,
            "rank": self._rank.to_dict() if self._rank is not None else None,
            "group_by": self._group_by.to_dict(),
            "limit": self._limit.to_dict(),
            "select": self._select.to_dict(),
        }

    # Builder methods for chaining
    def select_all(self) -> "Search":
        """Select all predefined keys (document, embedding, metadata, score)"""
        new_select = Select(keys={Key.DOCUMENT, Key.EMBEDDING, Key.METADATA, Key.SCORE})
        return Search(
            where=self._where,
            rank=self._rank,
            group_by=self._group_by,
            limit=self._limit,
            select=new_select,
        )

    def select(self, *keys: Union[Key, str]) -> "Search":
        """Select specific keys

        Args:
            *keys: Variable number of Key objects or string key names

        Returns:
            New Search object with updated select configuration
        """
        new_select = Select(keys=set(keys))
        return Search(
            where=self._where,
            rank=self._rank,
            group_by=self._group_by,
            limit=self._limit,
            select=new_select,
        )

    def where(self, where: Optional[Union[Where, Dict[str, Any]]]) -> "Search":
        """Set the where clause for filtering

        Args:
            where: A Where expression, dict, or None for filtering
                   Dicts will be converted using Where.from_dict()

        Example:
            search.where((Key("status") == "active") & (Key("score") > 0.5))
            search.where({"status": "active"})
            search.where({"$and": [{"status": "active"}, {"score": {"$gt": 0.5}}]})
        """
        return Search(
            where=where,
            rank=self._rank,
            group_by=self._group_by,
            limit=self._limit,
            select=self._select,
        )

    def rank(self, rank_expr: Optional[Union[Rank, Dict[str, Any]]]) -> "Search":
        """Set the ranking expression

        Args:
            rank_expr: A Rank expression, dict, or None for scoring
                       Dicts will be converted using Rank.from_dict()
                       Note: Primitive numbers are not accepted - use {"$val": number} for constant ranks

        Example:
            search.rank(Knn(query=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2)
            search.rank({"$knn": {"query": [0.1, 0.2]}})
            search.rank({"$sum": [{"$knn": {"query": [0.1, 0.2]}}, {"$val": 0.5}]})
        """
        return Search(
            where=self._where,
            rank=rank_expr,
            group_by=self._group_by,
            limit=self._limit,
            select=self._select,
        )

    def group_by(self, group_by: Optional[Union[GroupBy, Dict[str, Any]]]) -> "Search":
        """Set the group_by configuration for grouping and aggregating results

        Args:
            group_by: A GroupBy object, dict, or None for grouping
                      Dicts will be converted using GroupBy.from_dict()

        Example:
            search.group_by(GroupBy(
                keys=[Key("category")],
                aggregate=MinK(keys=[Key.SCORE], k=3)
            ))
            search.group_by({
                "keys": ["category"],
                "aggregate": {"$min_k": {"keys": ["#score"], "k": 3}}
            })
        """
        return Search(
            where=self._where,
            rank=self._rank,
            group_by=group_by,
            limit=self._limit,
            select=self._select,
        )

    def limit(self, limit: int, offset: int = 0) -> "Search":
        """Set the limit and offset for pagination

        Args:
            limit: Maximum number of results to return
            offset: Number of results to skip (default: 0)

        Example:
            search.limit(20, offset=10)
        """
        new_limit = Limit(offset=offset, limit=limit)
        return Search(
            where=self._where,
            rank=self._rank,
            group_by=self._group_by,
            limit=new_limit,
            select=self._select,
        )
