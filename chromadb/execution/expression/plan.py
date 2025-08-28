from dataclasses import dataclass, field, replace
from typing import List, Dict, Any, Union, Set

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


@dataclass
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
            .where(F("status") == "active")
            .rank_by(Knn(embedding=[0.1, 0.2]))
            .limit_by(10)
            .select_fields(SelectField.DOCUMENT))
    
    Empty Search() is valid and will use defaults:
        - filter: Empty SearchFilter (no filtering)
        - rank: Val(0.0) (constant score of 0)
        - limit: No limit
        - select: Empty selection
    """
    filter: SearchFilter = field(default_factory=SearchFilter)
    rank: Rank = field(default_factory=lambda: Val(value=0.0))
    limit: Limit = field(default_factory=Limit)
    select: Select = field(default_factory=Select)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the Search to a dictionary for JSON serialization"""
        return {
            "filter": self.filter.to_dict(),
            "rank": self.rank.to_dict(),
            "limit": self.limit.to_dict(),
            "select": self.select.to_dict()
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
        return replace(self, select=new_select)
    
    def select_fields(self, *fields: Union[SelectField, str]) -> 'Search':
        """Select specific fields
        
        Args:
            *fields: Variable number of SelectField enums or string field names
            
        Example:
            search.select_fields(SelectField.DOCUMENT, SelectField.SCORE, "title", "author")
        """
        new_select = Select(fields=set(fields))
        return replace(self, select=new_select)
    
    def where(self, where_clause: Where) -> 'Search':
        """Set the where clause for filtering
        
        Args:
            where_clause: A Where expression for filtering
            
        Example:
            search.where((F("status") == "active") & (F("score") > 0.5))
        """
        new_filter = replace(self.filter, where_clause=where_clause)
        return replace(self, filter=new_filter)
    
    def rank_by(self, rank_expr: Rank) -> 'Search':
        """Set the ranking expression
        
        Args:
            rank_expr: A Rank expression for scoring
            
        Example:
            search.rank_by(Knn(embedding=[0.1, 0.2]) * 0.8 + Val(0.5) * 0.2)
        """
        return replace(self, rank=rank_expr)
    
    def limit_by(self, limit: int, offset: int = 0) -> 'Search':
        """Set the limit and offset for pagination
        
        Args:
            limit: Maximum number of results to return
            offset: Number of results to skip (default: 0)
            
        Example:
            search.limit_by(20, offset=10)
        """
        new_limit = Limit(offset=offset, limit=limit)
        return replace(self, limit=new_limit)
