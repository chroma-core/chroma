from __future__ import annotations

import datetime
import hashlib
import json
import os
import pathlib
import platform
import sys
from typing import Any, Dict, List, Optional, Sequence

import pytest


_local_nodeids: List[str] = []
_xdist_nodeids_by_worker: Dict[str, List[str]] = {}


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("chroma-test-inventory")
    group.addoption(
        "--chroma-test-inventory-json",
        action="store",
        default=None,
        help="Write a JSON inventory of collected pytest node IDs.",
    )


def pytest_collection_finish(session: pytest.Session) -> None:
    if _is_xdist_worker(session.config):
        return

    global _local_nodeids
    _local_nodeids = [item.nodeid for item in session.items]


@pytest.hookimpl(optionalhook=True)
def pytest_xdist_node_collection_finished(node: Any, ids: Sequence[str]) -> None:
    worker_id = _worker_id(node)
    _xdist_nodeids_by_worker[worker_id] = list(ids)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _is_xdist_worker(session.config):
        return

    output_path = _output_path(session.config)
    if output_path is None:
        return

    nodeids, xdist = _collection()
    payload = {
        "schema_version": 1,
        "kind": "chroma-python-test-inventory-shard",
        "generated_at": _utc_now(),
        "shard": _shard_metadata(),
        "pytest": {
            "args": sys.argv[1:],
            "exitstatus": int(exitstatus),
        },
        "collection": {
            "count": len(nodeids),
            "sha256": _nodeids_sha256(nodeids),
            "tests": nodeids,
        },
        "xdist": xdist,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    with tmp_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")
    tmp_path.replace(output_path)


def _output_path(config: pytest.Config) -> Optional[pathlib.Path]:
    option_path = config.getoption("--chroma-test-inventory-json")
    path = option_path or os.getenv("CHROMA_TEST_INVENTORY_JSON")
    if not path:
        return None
    return pathlib.Path(path)


def _is_xdist_worker(config: pytest.Config) -> bool:
    return hasattr(config, "workerinput")


def _collection() -> tuple[List[str], Dict[str, Any]]:
    if not _xdist_nodeids_by_worker:
        return list(_local_nodeids), {
            "enabled": False,
            "worker_count": 0,
            "collections_consistent": True,
            "worker_collection_counts": {},
        }

    worker_ids = sorted(_xdist_nodeids_by_worker)
    first_worker = worker_ids[0]
    first_collection = _xdist_nodeids_by_worker[first_worker]
    collections_consistent = all(
        _xdist_nodeids_by_worker[worker_id] == first_collection
        for worker_id in worker_ids
    )

    if collections_consistent:
        nodeids = list(first_collection)
    else:
        nodeids = sorted(
            {
                nodeid
                for worker_nodeids in _xdist_nodeids_by_worker.values()
                for nodeid in worker_nodeids
            }
        )

    return nodeids, {
        "enabled": True,
        "worker_count": len(worker_ids),
        "collections_consistent": collections_consistent,
        "worker_collection_counts": {
            worker_id: len(_xdist_nodeids_by_worker[worker_id])
            for worker_id in worker_ids
        },
    }


def _worker_id(node: Any) -> str:
    workerinput = getattr(node, "workerinput", None)
    if isinstance(workerinput, dict):
        worker_id = workerinput.get("workerid")
        if worker_id:
            return str(worker_id)

    gateway = getattr(node, "gateway", None)
    gateway_id = getattr(gateway, "id", None)
    if gateway_id:
        return str(gateway_id)

    return f"worker-{len(_xdist_nodeids_by_worker)}"


def _shard_metadata() -> Dict[str, Optional[str]]:
    return {
        "job": os.getenv("CHROMA_TEST_INVENTORY_JOB") or os.getenv("GITHUB_JOB"),
        "python": os.getenv("CHROMA_TEST_INVENTORY_PYTHON")
        or f"{sys.version_info.major}.{sys.version_info.minor}",
        "runner_os": os.getenv("RUNNER_OS"),
        "runner_name": os.getenv("RUNNER_NAME"),
        "runner_input": os.getenv("CHROMA_TEST_INVENTORY_RUNNER"),
        "test_target": os.getenv("CHROMA_TEST_INVENTORY_TARGET"),
        "workflow": os.getenv("GITHUB_WORKFLOW"),
        "run_id": os.getenv("GITHUB_RUN_ID"),
        "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT"),
        "sha": os.getenv("GITHUB_SHA"),
        "ref": os.getenv("GITHUB_REF"),
        "platform": platform.platform(),
    }


def _nodeids_sha256(nodeids: Sequence[str]) -> str:
    hasher = hashlib.sha256()
    for nodeid in nodeids:
        hasher.update(nodeid.encode("utf-8"))
        hasher.update(b"\0")
    return hasher.hexdigest()


def _utc_now() -> str:
    return (
        datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )
