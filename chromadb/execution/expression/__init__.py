"""
Chromadb execution expression module for search operations.
"""

from chromadb.execution.expression.operator import (
    # Core filter components
    SearchFilter,
    
    # Field proxy for building Where conditions
    Key,
    K,
    Doc,
    
    # Where expressions
    Where,
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Nin,
    Regex,
    NotRegex,
    Contains,
    NotContains,
    
    # Search configuration
    Limit,
    Select,
    SelectField,
    
    # Rank expressions
    Rank,
    Abs,
    Div,
    Exp,
    Log,
    Max,
    Min,
    Mul,
    Knn,
    Sub,
    Sum,
    Val,
)

from chromadb.execution.expression.plan import (
    Search,
)

__all__ = [
    # Main search class
    "Search",
    
    # Filter components
    "SearchFilter",
    
    # Field proxy
    "Key",
    "K",
    "Doc",
    
    # Where expressions
    "Where",
    "And",
    "Or",
    "Eq",
    "Ne",
    "Gt",
    "Gte",
    "Lt",
    "Lte",
    "In",
    "Nin",
    "Regex",
    "NotRegex",
    "Contains",
    "NotContains",
    
    # Search configuration
    "Limit",
    "Select",
    "SelectField",
    
    # Rank expressions
    "Rank",
    "Abs",
    "Div",
    "Exp",
    "Log",
    "Max",
    "Min",
    "Mul",
    "Knn",
    "Sub",
    "Sum",
    "Val",
]