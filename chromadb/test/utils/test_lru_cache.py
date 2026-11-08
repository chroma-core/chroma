import pytest

from chromadb.utils.lru_cache import LRUCache


def test_evicts_least_recently_used() -> None:
    cache: LRUCache[str, int] = LRUCache(2)
    cache.set("a", 1)
    cache.set("b", 2)
    cache.get("a")  # "a" becomes the most recently used
    cache.set("c", 3)

    assert cache.get("a") == 1
    assert cache.get("b") is None
    assert cache.get("c") == 3


def test_eviction_invokes_callback() -> None:
    evicted: list[tuple[str, int]] = []
    cache: LRUCache[str, int] = LRUCache(
        1, callback=lambda k, v: evicted.append((k, v))
    )
    cache.set("a", 1)
    cache.set("b", 2)

    assert evicted == [("a", 1)]


def test_overwriting_a_key_does_not_evict() -> None:
    evicted: list[str] = []
    cache: LRUCache[str, int] = LRUCache(1, callback=lambda k, _: evicted.append(k))
    cache.set("a", 1)
    cache.set("a", 2)

    assert cache.get("a") == 2
    assert evicted == []


@pytest.mark.parametrize("capacity", [0, -1])
def test_capacity_below_one_is_rejected(capacity: int) -> None:
    """A zero capacity used to raise KeyError on the first set(), a negative one never evicted.

    LocalSegmentManager derives the capacity from
    RLIMIT_NOFILE // PersistentLocalHnswSegment.get_file_handle_count(), which floors to 0 when
    the file-descriptor limit is lower than the per-segment handle count.
    """
    with pytest.raises(ValueError, match="capacity must be at least 1"):
        LRUCache(capacity)


def test_capacity_one_holds_a_single_entry() -> None:
    cache: LRUCache[str, int] = LRUCache(1)
    cache.set("a", 1)
    cache.set("b", 2)

    assert cache.get("a") is None
    assert cache.get("b") == 2
