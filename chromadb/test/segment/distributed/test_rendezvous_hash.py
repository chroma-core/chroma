from chromadb.utils.rendezvous_hash import assign, murmur3hasher


def test_rendezvous_hash() -> None:
    # Tests the assign works as expected
    members = ["a", "b", "c"]
    key = "key"

    def mock_hasher(member: str, key: str) -> int:
        return members.index(member)  # Highest index wins

    assert assign(key, members, mock_hasher) == "c"


def test_even_distribution() -> None:
    member_count = 10
    tolerance = 25
    nodes = [str(i) for i in range(member_count)]

    # Test if keys are evenly distributed across nodes
    key_distribution = {node: 0 for node in nodes}
    num_keys = 1000
    for i in range(num_keys):
        key = f"key_{i}"
        node = assign(key, nodes, murmur3hasher)
        key_distribution[node] += 1

    # Check if keys are somewhat evenly distributed
    for node in nodes:
        assert abs(key_distribution[node] - num_keys / len(nodes)) < tolerance
