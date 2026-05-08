import os
import subprocess
from typing import cast, List

import hypothesis
import pytest
import hypothesis.strategies as st
from hypothesis import given, settings
from chromadb.api import ClientAPI
from chromadb.api.types import Embeddings, Metadatas
from chromadb.test.conftest import (
    MULTI_REGION_ENABLED,
    NOT_CLUSTER_ONLY,
    create_isolated_database,
    multi_region_test,
)
import chromadb.test.property.strategies as strategies
import chromadb.test.property.invariants as invariants
from chromadb.test.property.recordset_utils import create_large_recordset
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
from chromadb.utils.batch_utils import create_batches

REPO_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

CHROMA_FAULT_CMD = ["cargo", "run", "--bin", "chroma-fault", "--"]

collection_st = st.shared(strategies.collections(with_hnsw_params=True), key="coll")


def _inject_fault(label: str, tilt_instance: str = "chroma") -> None:
    """Inject an unavailable fault on the given label via chroma-fault CLI."""
    result = subprocess.run(
        CHROMA_FAULT_CMD
        + [
            "--tilt-instance",
            tilt_instance,
            "inject",
            "--label",
            label,
            "--unavailable",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, f"Failed to inject fault: {result.stderr}"
    print(f"Injected fault: {result.stdout.strip()}")


def _clear_faults(tilt_instance: str = "chroma") -> None:
    """Clear all injected faults via chroma-fault CLI."""
    result = subprocess.run(
        CHROMA_FAULT_CMD
        + [
            "--tilt-instance",
            tilt_instance,
            "clear",
            "--all",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, f"Failed to clear faults: {result.stderr}"
    print(f"Cleared faults: {result.stdout.strip()}")


@pytest.mark.skipif(
    NOT_CLUSTER_ONLY,
    reason="Fault injection requires a running Kubernetes cluster",
)
@multi_region_test
@given(collection=collection_st)
@settings(deadline=None, max_examples=1)
def test_add_large_with_fault_injection(
    client: ClientAPI,
    collection: strategies.Collection,
) -> None:
    """Fault-injected variant of test_add_large.

    Injects an unavailable fault on replica 0, runs a large add, clears the
    fault, and waits for compaction.  A background ``kubectl logs`` process
    captures log-service output so we can verify that 'Unavailable' and
    'read repair' messages appear.

    Per-replica fault labels (``wal3.fragment_upload.0``) are only checked in
    the replicated write path, which requires a topology-enabled database.
    The ``@multi_region_test`` decorator ensures the database gets a topology
    prefix (e.g. ``tilt-spanning+...``) so the log service routes through
    ``ReplicatedFragmentManager``.
    """
    if not MULTI_REGION_ENABLED:
        pytest.skip("Per-replica fault injection requires multi-region topology")
    create_isolated_database(client)

    # Capture only new log lines produced during this test.
    kubectl_proc = subprocess.Popen(
        [
            "kubectl",
            "logs",
            "-n",
            "chroma",
            "rust-log-service-0",
            "--tail=0",
            "--follow",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    kubectl_stdout = ""
    try:
        _inject_fault("wal3.fragment_upload.0")

        record_set = create_large_recordset(min_size=10000, max_size=50000)
        coll = client.create_collection(
            name=collection.name,
            metadata=collection.metadata,  # type: ignore
            embedding_function=collection.embedding_function,
        )
        normalized_record_set = invariants.wrap_all(record_set)
        initial_version = cast(int, coll.get_model()["version"])

        for batch in create_batches(
            api=client,
            ids=cast(List[str], record_set["ids"]),
            embeddings=cast(Embeddings, record_set["embeddings"]),
            metadatas=cast(Metadatas, record_set["metadatas"]),
            documents=cast(List[str], record_set["documents"]),
        ):
            coll.add(*batch)

        # Clear faults before compaction so read repair can succeed on the
        # previously-failed replica.
        _clear_faults()

        if len(normalized_record_set["ids"]) > 10:
            wait_for_version_increase(
                client, collection.name, initial_version, additional_time=240
            )

        invariants.count(coll, cast(strategies.RecordSet, normalized_record_set))
    finally:
        try:
            _clear_faults()
        except Exception:
            pass

        kubectl_proc.terminate()
        try:
            stdout, stderr = kubectl_proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            kubectl_proc.kill()
            stdout, stderr = kubectl_proc.communicate()
        kubectl_stdout = stdout

        print(f"kubectl logs captured {len(kubectl_stdout)} bytes")
        if stderr:
            print(f"kubectl stderr: {stderr[:500]}")

    lower = kubectl_stdout.lower()
    found_unavailable = "unavailable" in lower
    found_read_repair = "read repair" in lower or "read_repair" in lower
    print(f"Found 'Unavailable' in logs: {found_unavailable}")
    print(f"Found 'read repair' in logs: {found_read_repair}")
    assert found_unavailable, (
        "Expected 'Unavailable' in kubectl logs but did not find it"
    )
    assert found_read_repair, (
        "Expected 'read repair' in kubectl logs but did not find it"
    )
