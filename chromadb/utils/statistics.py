"""Utility functions for managing collection statistics.

This module provides standalone functions for enabling, disabling, and retrieving
statistics for ChromaDB collections. These functions work with the attached function
system to automatically compute metadata value frequencies.

Example:
    >>> from chromadb.utils.statistics import attach_statistics_function, get_statistics
    >>> import chromadb
    >>>
    >>> client = chromadb.Client()
    >>> collection = client.get_or_create_collection("my_collection")
    >>>
    >>> # Attach statistics function
    >>> attach_statistics_function(collection)
    >>>
    >>> # Add some data
    >>> collection.add(
    ...     ids=["id1", "id2"],
    ...     documents=["doc1", "doc2"],
    ...     metadatas=[{"category": "A"}, {"category": "B"}]
    ... )
    >>>
    >>> # Get statistics
    >>> stats = get_statistics(collection)
    >>> print(stats)
"""

from typing import TYPE_CHECKING, Optional, Dict, Any, cast
from collections import defaultdict

from chromadb.api.types import Where

if TYPE_CHECKING:
    from chromadb.api.models.Collection import Collection
    from chromadb.api.models.AttachedFunction import AttachedFunction


def get_statistics_fn_name(collection: "Collection") -> str:
    """Generate the default name for the statistics attached function.

    Args:
        collection: The collection to generate the name for

    Returns:
        str: The statistics function name
    """
    return f"{collection.name}_stats"


def attach_statistics_function(
    collection: "Collection", stats_collection_name: Optional[str] = None
) -> "AttachedFunction":
    """Attach statistics collection function to a collection.

    This attaches the statistics function which will automatically compute
    and update metadata value frequencies whenever records are added, updated,
    or deleted.

    Args:
        collection: The collection to enable statistics for
        stats_collection_name: Name of the collection where statistics will be stored.
                               If None, defaults to "{collection_name}_statistics".

    Returns:
        AttachedFunction: The attached statistics function

    Example:
        >>> attach_statistics_function(collection)
        >>> collection.add(ids=["id1"], documents=["doc1"], metadatas=[{"key": "value"}])
        >>> # Statistics are automatically computed
        >>> stats = get_statistics(collection)
    """
    if stats_collection_name is None:
        stats_collection_name = f"{collection.name}_statistics"

    return collection.attach_function(
        name=get_statistics_fn_name(collection),
        function_id="statistics",
        output_collection=stats_collection_name,
        params=None,
    )


def get_statistics_fn(collection: "Collection") -> "AttachedFunction":
    """Get the statistics attached function for a collection.

    Args:
        collection: The collection to get the statistics function for

    Returns:
        AttachedFunction: The statistics function

    Raises:
        NotFoundError: If statistics are not enabled
        AssertionError: If the attached function is not a statistics function
    """
    af = collection.get_attached_function(get_statistics_fn_name(collection))
    assert (
        af.function_name == "statistics"
    ), "Attached function is not a statistics function"
    return af


def detach_statistics_function(
    collection: "Collection", delete_stats_collection: bool = False
) -> bool:
    """Detach statistics collection function from a collection.

    Args:
        collection: The collection to disable statistics for
        delete_stats_collection: If True, also delete the statistics output collection.
                                  Defaults to False.

    Returns:
        bool: True if successful

    Example:
        >>> detach_statistics_function(collection, delete_stats_collection=True)
    """
    attached_fn = get_statistics_fn(collection)
    return collection.detach_function(
        attached_fn.name, delete_output_collection=delete_stats_collection
    )


def get_statistics(
    collection: "Collection", key: Optional[str] = None
) -> Dict[str, Any]:
    """Get the current statistics for a collection.

    Statistics include frequency counts for all metadata key-value pairs,
    as well as a summary with the total record count.

    Args:
        collection: The collection to get statistics for
        key: Optional metadata key to filter statistics for. If provided,
             only returns statistics for that specific key.

    Returns:
        Dict[str, Any]: A dictionary with the structure:
            {
                "statistics": {
                    "key1": {
                        "value1": {"count": count, ...},
                        "value2": {"count": count, ...}
                    },
                    "key2": {...},
                    ...
                },
                "summary": {
                    "total_count": count
                }
            }

    Example:
        >>> attach_statistics_function(collection)
        >>> collection.add(
        ...     ids=["id1", "id2"],
        ...     documents=["doc1", "doc2"],
        ...     metadatas=[{"category": "A", "score": 10}, {"category": "B", "score": 10}]
        ... )
        >>> # Wait for statistics to be computed
        >>> stats = get_statistics(collection)
        >>> print(stats)
        {
            "statistics": {
                "category": {
                    "A": {"count": 1},
                    "B": {"count": 1}
                },
                "score": {
                    "10": {"count": 2}
                }
            },
            "summary": {
                "total_count": 2
            }
        }
    """
    # Import here to avoid circular dependency
    from chromadb.api.models.Collection import Collection

    af = get_statistics_fn(collection)

    # Get the statistics output collection model from the server
    stats_collection_model = collection._client.get_collection(
        name=af.output_collection,
        tenant=collection.tenant,
        database=collection.database,
    )

    # Wrap it in a Collection object to access get/query methods
    stats_collection = Collection(
        client=collection._client,
        model=stats_collection_model,
        embedding_function=None,  # Statistics collections don't need embedding functions
        data_loader=None,
    )

    # Get all statistics records by paginating through the stats collection
    stats: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(lambda: defaultdict(dict))
    summary: Dict[str, Any] = {}

    offset = 0
    # When filtering by key, also include "summary" entries to get total_count
    where_filter: Optional[Where] = (
        cast(Where, {"$or": [{"key": key}, {"key": "summary"}]})
        if key is not None
        else None
    )

    while True:
        page = stats_collection.get(
            include=["metadatas"], offset=offset, where=where_filter
        )

        metadatas = page.get("metadatas") or []
        if not metadatas:
            break

        for metadata in metadatas:
            if metadata is None:
                continue

            meta_key = metadata.get("key")
            value = metadata.get("value")
            value_label = metadata.get("value_label")
            value_type = metadata.get("type")
            count = metadata.get("count")

            if (
                meta_key is not None
                and value is not None
                and value_type is not None
                and count is not None
            ):
                if meta_key == "summary":
                    if value == "total_count":
                        summary["total_count"] = count
                else:
                    # Prioritize value_label if present, otherwise use value
                    stats_key = value_label if value_label is not None else value
                    assert isinstance(meta_key, str)
                    assert isinstance(stats_key, str)
                    assert isinstance(count, int)
                    stats[meta_key][stats_key]["count"] = count

        # Advance to next page using the actual number of items returned
        offset += len(metadatas)

    result = {"statistics": dict(stats)}
    if summary:
        result["summary"] = summary

    return result
