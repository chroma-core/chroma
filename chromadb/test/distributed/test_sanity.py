# This tests a very minimal of test_add in test_add.py as a example based test
# instead of a property based test. We can use the delta to get the property
# test working and then enable
import random
from typing import List
from chromadb.api import ServerAPI
import time

from chromadb.api.types import QueryResult
from chromadb.test.conftest import COMPACTION_SLEEP, MEMBERLIST_SLEEP

EPS = 1e-6


def test_add(
    api: ServerAPI,
) -> None:
    api.reset()

    # Once we reset, we have to wait for sometime to let the memberlist on the frontends
    # propagate, there isn't a clean way to do this so we sleep for a configured amount of time
    # to ensure that the memberlist has propagated
    time.sleep(MEMBERLIST_SLEEP)

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
        include=["distances"],
    )

    # Check that the distances are correct in l2
    ground_truth_distances = [
        sum((a - b) ** 2 for a, b in zip(embedding, random_query))
        for embedding in embeddings
    ]
    ground_truth_distances.sort()
    retrieved_distances = results["distances"][0]  # type: ignore

    # Check that the query results are sorted by distance
    for i in range(1, len(retrieved_distances)):
        assert retrieved_distances[i - 1] <= retrieved_distances[i]

    for i in range(len(retrieved_distances)):
        assert abs(ground_truth_distances[i] - retrieved_distances[i]) < EPS


def test_add_include_all_with_compaction_delay(api: ServerAPI) -> None:
    api.reset()

    time.sleep(MEMBERLIST_SLEEP)

    collection = api.create_collection(
        name="test_add_include_all_with_compaction_delay"
    )

    ids = []
    embeddings = []
    for i in range(1000):
        ids.append(str(i))
        embeddings.append([random.random(), random.random(), random.random()])
        collection.add(
            ids=[str(i)],
            embeddings=[embeddings[-1]],  # type: ignore
            documents=f"document_{i}",
        )

    time.sleep(COMPACTION_SLEEP)  # Wait for the documents to be compacted

    random_query_1 = [random.random(), random.random(), random.random()]
    random_query_2 = [random.random(), random.random(), random.random()]

    # Query the collection with a random query
    results = collection.query(
        query_embeddings=[random_query_1, random_query_2],  # type: ignore
        n_results=10,
        include=["metadatas", "documents", "distances", "embeddings"],
    )

    ids_and_embeddings = list(zip(ids, embeddings))

    def validate(results: QueryResult, query: List[float], result_index: int) -> None:
        # Check that the distances are correct in l2
        gt_ids_distances_embeddings = [
            (id, sum((a - b) ** 2 for a, b in zip(embedding, query)), embedding)
            for id, embedding in ids_and_embeddings
        ]
        gt_ids_distances_embeddings.sort(key=lambda x: x[1])
        retrieved_distances = results["distances"][result_index]  # type: ignore

        # Check that the query results are sorted by distance
        for i in range(1, len(retrieved_distances)):
            assert retrieved_distances[i - 1] <= retrieved_distances[i]

        for i in range(len(retrieved_distances)):
            assert abs(gt_ids_distances_embeddings[i][1] - retrieved_distances[i]) < EPS

        # Check that the ids are correct
        retrieved_ids = results["ids"][result_index]
        for i in range(len(retrieved_ids)):
            assert retrieved_ids[i] == gt_ids_distances_embeddings[i][0]

        # Check that the documents are correct
        if "documents" in results and results["documents"] is not None:
            retrieved_documents = results["documents"][result_index]
            for i in range(len(retrieved_documents)):
                assert (
                    retrieved_documents[i]
                    == f"document_{gt_ids_distances_embeddings[i][0]}"
                )
        else:
            assert False

        # Check that the embeddings are correct
        if "embeddings" in results and results["embeddings"] is not None:
            retrieved_embeddings = results["embeddings"][result_index]
            for i in range(len(retrieved_embeddings)):
                # eps compare the embeddings
                for j in range(3):
                    assert (
                        abs(
                            retrieved_embeddings[i][j]
                            - gt_ids_distances_embeddings[i][2][j]
                        )
                        < EPS
                    )
        else:
            assert False

    validate(results, random_query_1, 0)
    validate(results, random_query_2, 1)
