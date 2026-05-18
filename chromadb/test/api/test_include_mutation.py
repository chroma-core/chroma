"""
Tests for GitHub issue #5857: In-place mutation of `include` parameter.

The `include` list passed to `query()` and `get()` must not be mutated
in-place so that callers who reuse the same list object across multiple
calls get consistent results.
"""

from chromadb.api import ClientAPI


def test_query_does_not_mutate_include_list(client: ClientAPI) -> None:
    """query() must not mutate the caller-supplied include list."""
    client.reset()
    collection = client.create_collection(name="test_include_mutation_query")
    collection.add(ids=["1", "2"], documents=["hello world", "foo bar"])

    include = ["documents", "distances"]
    original_include = list(include)  # snapshot before any call

    # First call
    results1 = collection.query(query_texts=["hello"], n_results=1, include=include)
    assert include == original_include, (
        f"query() mutated the include list on first call: {include!r}"
    )

    # Second call with the same list — should produce identical structure
    results2 = collection.query(query_texts=["hello"], n_results=1, include=include)
    assert include == original_include, (
        f"query() mutated the include list on second call: {include!r}"
    )

    # Results from both calls should be structurally equivalent
    assert results1["ids"] == results2["ids"]
    assert results1.get("documents") == results2.get("documents")


def test_get_does_not_mutate_include_list(client: ClientAPI) -> None:
    """get() must not mutate the caller-supplied include list."""
    client.reset()
    collection = client.create_collection(name="test_include_mutation_get")
    collection.add(ids=["1", "2"], documents=["hello world", "foo bar"])

    include = ["documents"]
    original_include = list(include)

    # First call
    results1 = collection.get(ids=["1"], include=include)
    assert include == original_include, (
        f"get() mutated the include list on first call: {include!r}"
    )

    # Second call
    results2 = collection.get(ids=["1"], include=include)
    assert include == original_include, (
        f"get() mutated the include list on second call: {include!r}"
    )

    assert results1["ids"] == results2["ids"]
    assert results1.get("documents") == results2.get("documents")


def test_query_with_data_include_does_not_leak_uris(client: ClientAPI) -> None:
    """When 'data' is in include, the internally-added 'uris' must not leak
    back into the caller's list."""
    client.reset()
    collection = client.create_collection(name="test_include_data_no_leak")
    collection.add(ids=["1"], documents=["hello world"])

    # 'data' triggers the internal URI augmentation path
    include = ["documents", "distances"]
    original_include = list(include)

    collection.query(query_texts=["hello"], n_results=1, include=include)

    assert include == original_include, (
        f"query() leaked internal state back into caller's include: {include!r}"
    )
