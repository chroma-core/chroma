"""Attachable function definitions for ChromaDB collections.

This module provides function constants that can be attached to collections
to perform automatic computations on collection data.

Example:
    >>> from chromadb.api.functions import STATISTICS_FUNCTION
    >>> attached_fn = collection.attach_function(
    ...     function=STATISTICS_FUNCTION,
    ...     name="my_stats",
    ...     output_collection="my_stats_output"
    ... )
"""

from enum import Enum


class Function(str, Enum):
    """Available functions that can be attached to collections."""

    STATISTICS = "statistics"
    """Computes metadata value frequencies for a collection."""

    RECORD_COUNTER = "record_counter"
    """Counts records in a collection."""

    REVISION_HISTORY = "revision_history"
    """Archives every version of a record into a lightweight history collection."""

    DUMMY_ASYNC = "dummy_async"
    """Async test helper function used for distributed task API coverage."""

    COUNT_TO_FILE_ASYNC = "count_to_file_async"
    """Async test helper that writes a running count to a configured MinIO path."""

    # Used only for failure testing - not a real function
    _NONEXISTENT_TEST_ONLY = "nonexistent_function"


# Convenience aliases for cleaner imports
STATISTICS_FUNCTION = Function.STATISTICS
RECORD_COUNTER_FUNCTION = Function.RECORD_COUNTER
REVISION_HISTORY_FUNCTION = Function.REVISION_HISTORY
DUMMY_ASYNC_FUNCTION = Function.DUMMY_ASYNC
COUNT_TO_FILE_ASYNC_FUNCTION = Function.COUNT_TO_FILE_ASYNC
