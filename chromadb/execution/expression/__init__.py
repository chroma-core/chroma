"""
Chromadb execution expression module for search operations.
"""

from chromadb.execution.expression.operator import (
    # Field proxy for building Where conditions
    Key,
    K,
    
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
    
    # Field proxy
    "Key",
    "K",
    
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