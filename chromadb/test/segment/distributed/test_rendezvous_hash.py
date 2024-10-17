from chromadb.utils.rendezvous_hash import assign, murmur3hasher


def test_rendezvous_hash() -> None:
    # Tests the assign works as expected
    members = ["a", "b", "c"]
    key = "key"

    def mock_hasher(member: str, key: str) -> int:
        return members.index(member)  # Highest index wins

    assert assign(key, members, mock_hasher, 1)[0] == "c"


def test_even_distribution() -> None:
    member_count = 10
    tolerance = 25
    nodes = [str(i) for i in range(member_count)]

    # Test if keys are evenly distributed across nodes
    key_distribution = {node: 0 for node in nodes}
    num_keys = 1000
    for i in range(num_keys):
        key = f"key_{i}"
        node = assign(key, nodes, murmur3hasher, 1)[0]
        key_distribution[node] += 1

    # Check if keys are somewhat evenly distributed
    for node in nodes:
        assert abs(key_distribution[node] - num_keys / len(nodes)) < tolerance


def test_multi_assign_even_distribution() -> None:
    member_count = 10
    tolerance = 75
    nodes = [str(i) for i in range(member_count)]

    # Test if keys are evenly distributed across nodes
    key_distribution = {node: 0 for node in nodes}
    num_keys = 10000
    replication = 3
    for i in range(num_keys):
        key = f"key_{i}"
        nodes_assigned = assign(key, nodes, murmur3hasher, replication)
        # Should be three unique nodes
        assert len(set(nodes_assigned)) == replication
        for node in nodes_assigned:
            key_distribution[node] += 1

    # Check if keys are somewhat evenly distributed
    for node in nodes:
        # 3k keys expected for each node (10000 keys / 10 nodes * 3 replication)
        expected = num_keys / len(nodes) * replication
        assert abs(key_distribution[node] - expected) < tolerance
