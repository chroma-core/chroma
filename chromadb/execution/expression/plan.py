from dataclasses import dataclass, field
from typing import List, Dict, Any, Union, Set, Optional

from chromadb.execution.expression.operator import (
    KNN, Filter, Limit, Projection, Scan, Rank, Select, Val, SearchFilter,
    SelectField, Where
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
    
    Direct construction:
        Search(
            filter=SearchFilter(where_clause=Eq("status", "active")),
            rank=Knn(embedding=[0.1, 0.2]),
            limit=Limit(limit=10),
            select=Select(fields={SelectField.DOCUMENT})
        )
    
    Builder pattern:
        (Search()
            .where(Key("status") == "active")
            .rank(Knn(embedding=[0.1, 0.2]))
            .limit(10)
            .select(SelectField.DOCUMENT))
    
    Empty Search() is valid and will use defaults:
        - filter: Empty SearchFilter (no filtering)
        - rank: Val(0.0) (constant score of 0)
        - limit: No limit
        - select: Empty selection
    """
    
    def __init__(
        self,
        filter: Optional[SearchFilter] = None,
        rank: Optional[Rank] = None,
        limit: Optional[Limit] = None,
        select: Optional[Select] = None
    ):
        """Initialize a Search with optional parameters.
        
        Args:
            filter: SearchFilter for filtering results (defaults to empty filter)
            rank: Rank expression for scoring (defaults to Val(0.0))
            limit: Limit configuration for pagination (defaults to no limit)
            select: Select configuration for fields (defaults to empty selection)
        """
        self._filter = filter if filter is not None else SearchFilter()
        self._rank = rank if rank is not None else Val(value=0.0)
        self._limit = limit if limit is not None else Limit()
        self._select = select if select is not None else Select()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Search to a dictionary for JSON serialization"""
        return {
            "filter": self._filter.to_dict(),
            "rank": self._rank.to_dict(),
            "limit": self._limit.to_dict(),
            "select": self._select.to_dict()
        }
    
    # Builder methods for chaining
    def select_all(self) -> 'Search':
        """Select all predefined fields (document, embedding, metadata, score)"""
        new_select = Select(fields={
            SelectField.DOCUMENT,
            SelectField.EMBEDDING,
            SelectField.METADATA,
            SelectField.SCORE
        })
        return Search(
            filter=self._filter,
            rank=self._rank,
            limit=self._limit,
            select=new_select
        )
    
    def select(self, *fields: Union[SelectField, str]) -> 'Search':
        """Select specific fields
        
        Args:
            *fields: Variable number of SelectField enums or string field names
            
        Example:
            search.select(SelectField.DOCUMENT, SelectField.SCORE, "title", "author")
        """
        new_select = Select(fields=set(fields))
        return Search(
            filter=self._filter,
            rank=self._rank,
            limit=self._limit,
            select=new_select
        )
    
    def where(self, where_clause: Where) -> 'Search':
        """Set the where clause for filtering
        
        Args:
            where_clause: A Where expression for filtering
            
        Example:
            search.where((Key("status") == "active") & (Key("score") > 0.5))
        """
        new_filter = SearchFilter(
            query_ids=self._filter.query_ids,
            where_clause=where_clause
        )
        return Search(
            filter=new_filter,
            rank=self._rank,
            limit=self._limit,
            select=self._select
        )
    
    def filter_by_ids(self, query_ids: List[str]) -> 'Search':
        """Filter results by specific IDs
        
        Args:
            query_ids: List of IDs to filter by
            
        Example:
            search.filter_by_ids(["id1", "id2", "id3"])
        """
        new_filter = SearchFilter(
            query_ids=query_ids,
            where_clause=self._filter.where_clause
        )
        return Search(
            filter=new_filter,
            rank=self._rank,
            limit=self._limit,
            select=self._select
        )
    
    def rank(self, rank_expr: Rank) -> 'Search':
        """Set the ranking expression
        
        Args:
            rank_expr: A Rank expression for scoring
            
        Example:
            search.rank(Knn(embedding=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2)
        """
        return Search(
            filter=self._filter,
            rank=rank_expr,
            limit=self._limit,
            select=self._select
        )
    
    def limit(self, limit: int, offset: int = 0) -> 'Search':
        """Set the limit and offset for pagination
        
        Args:
            limit: Maximum number of results to return
            offset: Number of results to skip (default: 0)
            
        Example:
            search.limit(20, offset=10)
        """
        new_limit = Limit(offset=offset, limit=limit)
        return Search(
            filter=self._filter,
            rank=self._rank,
            limit=new_limit,
            select=self._select
        )
