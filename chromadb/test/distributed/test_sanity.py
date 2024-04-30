# This tests a very minimal of test_add in test_add.py as a example based test
# instead of a property based test. We can use the delta to get the property
# test working and then enable
import random
from chromadb.api import ServerAPI
import time

EPS = 1e-6
SLEEP = 5


def test_add(
    api: ServerAPI,
) -> None:
    api.reset()

    # Once we reset, we have to wait for sometime to let the memberlist on the frontends
    # propagate, there isn't a clean way to do this so we sleep for a configured amount of time
    # to ensure that the memberlist has propagated
    time.sleep(SLEEP)

    collection = api.create_collection(
        name="test",
    )

    # Add 1000 records, where each embedding has 3 dimensions randomly generated
    # between 0 and 1
    ids = []
    embeddings = []
    for i in range(1000):
        ids.append(str(i))
        embeddings.append([random.random(), random.random(), random.random()])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],  # type: ignore
        )

    random_query = [random.random(), random.random(), random.random()]

    # Query the collection with a random query
    results = collection.query(
        query_embeddings=[random_query],  # type: ignore
        n_results=10,
    )

    # Check that the distances are correct in l2
    ground_truth_distances = [
        sum((a - b) ** 2 for a, b in zip(embedding, random_query))
        for embedding in embeddings
    ]
    ground_truth_distances.sort()
    retrieved_distances = results["distances"][0]  # type: ignore
    print("Ground truth distances: ", ground_truth_distances)
    print("Retrieved distances: ", retrieved_distances)

    # Check that the query results are sorted by distance
    for i in range(1, len(retrieved_distances)):
        assert retrieved_distances[i - 1] <= retrieved_distances[i]

    for i in range(len(retrieved_distances)):
        assert abs(ground_truth_distances[i] - retrieved_distances[i]) < EPS
