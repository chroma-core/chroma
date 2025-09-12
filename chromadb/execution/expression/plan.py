from dataclasses import dataclass, field
from typing import List, Dict, Any, Union, Set, Optional

from chromadb.execution.expression.operator import (
    KNN, Filter, Limit, Projection, Scan, Rank, Select, Val,
    Where, Key
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
            where=Key("status") == "active",
            rank=Knn(query=[0.1, 0.2]),
            limit=Limit(limit=10),
            select=Select(keys={Key.DOCUMENT})
        )
    
    Builder pattern:
        (Search()
            .where(Key("status") == "active")
            .rank(Knn(query=[0.1, 0.2]))
            .limit(10)
            .select(Key.DOCUMENT))
    
    Filter by IDs:
        Search().where(Key.ID.is_in(["id1", "id2", "id3"]))
    
    Combined with metadata filtering:
        Search().where((Key.ID.is_in(["id1", "id2"])) & (Key("status") == "active"))
    
    Empty Search() is valid and will use defaults:
        - where: None (no filtering)
        - rank: None (no ranking - results ordered by default order)
        - limit: No limit
        - select: Empty selection
    """
    
    def __init__(
        self,
        where: Optional[Where] = None,
        rank: Optional[Rank] = None,
        limit: Optional[Limit] = None,
        select: Optional[Select] = None
    ):
        """Initialize a Search with optional parameters.
        
        Args:
            where: Where expression for filtering results (defaults to None - no filtering)
            rank: Rank expression for scoring (defaults to None - no ranking)
            limit: Limit configuration for pagination (defaults to no limit)
            select: Select configuration for keys (defaults to empty selection)
        """
        self._where = where  # Keep as None if not provided
        self._rank = rank  # Keep as None if not provided
        self._limit = limit if limit is not None else Limit()
        self._select = select if select is not None else Select()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Search to a dictionary for JSON serialization"""
        return {
            "filter": self._where.to_dict() if self._where is not None else None,
            "rank": self._rank.to_dict() if self._rank is not None else None,
            "limit": self._limit.to_dict(),
            "select": self._select.to_dict()
        }
    
    # Builder methods for chaining
    def select_all(self) -> 'Search':
        """Select all predefined keys (document, embedding, metadata, score)"""
        new_select = Select(keys={
            Key.DOCUMENT,
            Key.EMBEDDING,
            Key.METADATA,
            Key.SCORE
        })
        return Search(
            where=self._where,
            rank=self._rank,
            limit=self._limit,
            select=new_select
        )
    
    def select(self, *keys: Union[Key, str]) -> 'Search':
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
            limit=self._limit,
            select=new_select
        )
    
    def where(self, where: Where) -> 'Search':
        """Set the where clause for filtering
        
        Args:
            where: A Where expression for filtering
            
        Example:
            search.where((Key("status") == "active") & (Key("score") > 0.5))
        """
        return Search(
            where=where,
            rank=self._rank,
            limit=self._limit,
            select=self._select
        )
    

    
    def rank(self, rank_expr: Rank) -> 'Search':
        """Set the ranking expression
        
        Args:
            rank_expr: A Rank expression for scoring
            
        Example:
            search.rank(Knn(query=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2)
        """
        return Search(
            where=self._where,
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
            where=self._where,
            rank=self._rank,
            limit=new_limit,
            select=self._select
        )
