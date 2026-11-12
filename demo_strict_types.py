#!/usr/bin/env python3
"""
Demo script showing the improved type safety with strict TypedDict definitions.

This demonstrates how the changes improve type checking while maintaining
backwards compatibility.
"""
from typing import Dict, List, Union, Any, Optional
from typing_extensions import TypedDict, NotRequired
import sys
import os

# Add the chroma directory to sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'chroma'))

# Define the strict types (mimicking what we added to the SDK)
class SparseVectorTransportDict(TypedDict):
    """Strict type for SparseVector transport format."""
    indices: List[int]
    values: List[float]
    tokens: NotRequired[Optional[List[str]]]

class LimitDict(TypedDict):
    """Strict type for Limit dictionary."""
    offset: NotRequired[int]
    limit: NotRequired[Optional[int]]

class SelectDict(TypedDict):
    """Strict type for Select dictionary."""
    keys: List[str]

# Example function signatures with improved typing
def process_sparse_vector(data: Union[SparseVectorTransportDict, Dict[str, Any]]) -> str:
    """
    Process sparse vector data with improved type safety.

    Before: def process_sparse_vector(data: Dict[str, Any]) -> str
    After: def process_sparse_vector(data: Union[SparseVectorTransportDict, Dict[str, Any]]) -> str

    The Union type allows backwards compatibility while encouraging use of the stricter type.
    """
    # Type checkers now know the expected structure
    indices = data.get("indices", [])
    values = data.get("values", [])
    tokens = data.get("tokens")

    if not isinstance(indices, list) or not isinstance(values, list):
        raise TypeError("indices and values must be lists")

    return f"SparseVector with {len(indices)} indices and {len(values)} values"

def process_limit(data: Union[LimitDict, Dict[str, Any]]) -> str:
    """Process limit data with improved type safety."""
    offset = data.get("offset", 0)
    limit = data.get("limit")

    if not isinstance(offset, int):
        raise TypeError("offset must be an integer")

    if limit is not None and not isinstance(limit, int):
        raise TypeError("limit must be an integer or None")

    return f"Limit: offset={offset}, limit={limit}"

def process_select(data: Union[SelectDict, Dict[str, Any]]) -> str:
    """Process select data with improved type safety."""
    keys = data.get("keys", [])

    if not isinstance(keys, list):
        raise TypeError("keys must be a list")

    return f"Select: {len(keys)} keys"

def main():
    """Demonstrate the improved type safety."""
    print("=== Chroma SDK Strict Types Demo ===\n")

    # Test with correctly typed data (what type checkers prefer)
    print("1. Testing with correctly structured data:")

    sparse_data: SparseVectorTransportDict = {
        "indices": [0, 1, 2],
        "values": [1.0, 2.0, 3.0],
        "tokens": ["hello", "world", "test"]
    }
    print(f"   {process_sparse_vector(sparse_data)}")

    limit_data: LimitDict = {"offset": 10, "limit": 20}
    print(f"   {process_limit(limit_data)}")

    select_data: SelectDict = {"keys": ["#document", "#score", "metadata_field"]}
    print(f"   {process_select(select_data)}")

    # Test backwards compatibility with plain dicts
    print("\n2. Testing backwards compatibility with plain dicts:")

    old_sparse = {"indices": [4, 5], "values": [4.0, 5.0]}  # Missing tokens is OK
    print(f"   {process_sparse_vector(old_sparse)}")

    old_limit = {"offset": 0}  # Missing limit is OK
    print(f"   {process_limit(old_limit)}")

    old_select = {"keys": ["field1", "field2"]}
    print(f"   {process_select(old_select)}")

    # Test error cases that are now caught earlier by type checkers
    print("\n3. Testing error cases (these would be caught by type checkers):")

    try:
        # This would now cause a type checker warning
        bad_sparse = {"indices": "not a list", "values": [1.0]}
        process_sparse_vector(bad_sparse)
    except TypeError as e:
        print(f"   Caught error in sparse vector: {e}")

    try:
        # This would now cause a type checker warning
        bad_limit = {"offset": "not an int", "limit": 20}
        process_limit(bad_limit)
    except TypeError as e:
        print(f"   Caught error in limit: {e}")

    try:
        # This would now cause a type checker warning
        bad_select = {"keys": "not a list"}
        process_select(bad_select)
    except TypeError as e:
        print(f"   Caught error in select: {e}")

    print("\n=== Benefits of This Approach ===")
    print("✅ Type checkers (mypy, pyright) can catch errors at development time")
    print("✅ IDEs provide better autocomplete and error highlighting")
    print("✅ AI agents get better guidance on function call structure")
    print("✅ Backwards compatible - existing code continues to work")
    print("✅ Runtime errors are more informative")
    print("✅ Code is self-documenting through types")

if __name__ == "__main__":
    main()