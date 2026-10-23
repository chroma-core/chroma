import datetime
import hashlib
import hmac
import selectors
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import uuid
from typing import Dict, List, Optional, Tuple, cast

import pytest

from chromadb.api import ClientAPI
from chromadb.api.models.Collection import Collection
from chromadb.api.types import Embeddings, Metadatas
from chromadb.test.conftest import MULTI_REGION_ENABLED
from chromadb.test.property.test_add_mcmr import (
    _create_isolated_database_mcmr,
    _create_mcmr_clients,
)
from chromadb.test.utils.wait_for_version_increase import wait_for_version_increase
from chromadb.utils.batch_utils import create_batches


GC_NAMESPACES = ("chroma", "chroma2")
GC_POD_NAME = "garbage-collector-0"
MINIO_S3_ENDPOINT = "http://localhost:9000"
MINIO_BUCKETS = ("chroma-storage", "chroma-storage2")
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
MINIO_REGION = "us-east-1"
COMPACTION_ROUNDS = 3
RECORDS_PER_ROUND = 25
GC_HARD_DELETE_TIMEOUT_SECONDS = 240
MINIO_OBJECT_APPEAR_TIMEOUT_SECONDS = 60
MINIO_OBJECT_DELETE_TIMEOUT_SECONDS = 60
MINIO_OBJECT_LIST_TIMEOUT_SECONDS = 30.0


def _records_for_round(
    round_index: int,
) -> Tuple[List[str], Embeddings, Metadatas, List[str]]:
    ids = [
        f"round-{round_index}-record-{record_index}-{uuid.uuid4()}"
        for record_index in range(RECORDS_PER_ROUND)
    ]
    embeddings = [
        [float(round_index), float(record_index), 1.0]
        for record_index in range(RECORDS_PER_ROUND)
    ]
    metadatas = [
        {"round": round_index, "record": record_index}
        for record_index in range(RECORDS_PER_ROUND)
    ]
    documents = [
        f"round {round_index} record {record_index}"
        for record_index in range(RECORDS_PER_ROUND)
    ]
    return ids, cast(Embeddings, embeddings), cast(Metadatas, metadatas), documents


def _add_round(client: ClientAPI, collection: Collection, round_index: int) -> None:
    ids, embeddings, metadatas, documents = _records_for_round(round_index)
    for batch in create_batches(
        api=client,
        ids=ids,
        embeddings=embeddings,
        metadatas=metadatas,
        documents=documents,
    ):
        collection.add(*batch)


def _start_gc_log_watchers() -> List[Tuple[str, subprocess.Popen]]:
    watchers = []
    for namespace in GC_NAMESPACES:
        try:
            proc = subprocess.Popen(
                [
                    "kubectl",
                    "logs",
                    "-n",
                    namespace,
                    GC_POD_NAME,
                    "--tail=0",
                    "--follow",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except FileNotFoundError:
            pytest.skip("kubectl is required to watch garbage collector logs")
        watchers.append((namespace, proc))
    return watchers


def _hard_delete_log_found(output: str, collection_uuid: str) -> bool:
    offset = 0
    while True:
        hard_delete_index = output.find("Hard deleting collections", offset)
        if hard_delete_index == -1:
            return False
        if collection_uuid in output[hard_delete_index : hard_delete_index + 4096]:
            return True
        offset = hard_delete_index + len("Hard deleting collections")


def _wait_for_gc_hard_delete_log(
    watchers: List[Tuple[str, subprocess.Popen]],
    captured_stdout: Dict[str, List[str]],
    collection_uuid: str,
) -> None:
    selector = selectors.DefaultSelector()
    try:
        for namespace, proc in watchers:
            if proc.stdout is None:
                continue
            selector.register(proc.stdout, selectors.EVENT_READ, namespace)

        deadline = time.monotonic() + GC_HARD_DELETE_TIMEOUT_SECONDS
        while time.monotonic() < deadline and selector.get_map():
            timeout = min(1.0, max(0.0, deadline - time.monotonic()))
            for key, _ in selector.select(timeout=timeout):
                namespace = cast(str, key.data)
                line = key.fileobj.readline()
                if line == "":
                    selector.unregister(key.fileobj)
                    continue
                captured_stdout[namespace].append(line)
                if _hard_delete_log_found(
                    "".join(captured_stdout[namespace]), collection_uuid
                ):
                    return
    finally:
        selector.close()


def _stop_gc_log_watchers(
    watchers: List[Tuple[str, subprocess.Popen]],
    captured_stdout: Dict[str, List[str]],
) -> Dict[str, str]:
    captured_stderr = {}
    for _, proc in watchers:
        proc.terminate()

    for namespace, proc in watchers:
        try:
            stdout, stderr = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
        if stdout:
            captured_stdout[namespace].append(stdout)
        captured_stderr[namespace] = stderr
    return captured_stderr


def _aws_quote(value: str) -> str:
    return urllib.parse.quote(value, safe="-_.~")


def _signing_key(date_stamp: str) -> bytes:
    date_key = hmac.new(
        f"AWS4{MINIO_SECRET_KEY}".encode("utf-8"),
        date_stamp.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    region_key = hmac.new(
        date_key, MINIO_REGION.encode("utf-8"), hashlib.sha256
    ).digest()
    service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
    return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


def _minio_signed_get(bucket: str, query: Dict[str, str]) -> bytes:
    endpoint = urllib.parse.urlparse(MINIO_S3_ENDPOINT)
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(b"").hexdigest()

    path = f"/{bucket}"
    canonical_uri = urllib.parse.quote(path, safe="/-_.~")
    canonical_query = "&".join(
        f"{_aws_quote(key)}={_aws_quote(value)}" for key, value in sorted(query.items())
    )
    canonical_headers = (
        f"host:{endpoint.netloc}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [
            "GET",
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )

    credential_scope = f"{date_stamp}/{MINIO_REGION}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(date_stamp),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        "AWS4-HMAC-SHA256 "
        f"Credential={MINIO_ACCESS_KEY}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )

    url = urllib.parse.urlunparse(
        (
            endpoint.scheme,
            endpoint.netloc,
            path,
            "",
            canonical_query,
            "",
        )
    )
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": authorization,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(
            request, timeout=MINIO_OBJECT_LIST_TIMEOUT_SECONDS
        ) as response:
            return response.read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        pytest.fail(
            "Failed to list MinIO objects from S3 API "
            f"{MINIO_S3_ENDPOINT}/{bucket}: HTTP {e.code} {e.reason}; body={body!r}"
        )
    except urllib.error.URLError as e:
        pytest.fail(
            "Failed to connect to MinIO S3 API "
            f"{MINIO_S3_ENDPOINT}/{bucket}: {e}"
        )


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _child_text(element: ET.Element, name: str) -> Optional[str]:
    for child in element:
        if _xml_local_name(child.tag) == name:
            return child.text
    return None


def _is_collection_version_file_key(key: str, collection_uuid: str) -> bool:
    return f"/collection/{collection_uuid}/versionfiles/" in key


def _list_minio_files_for_collection(bucket: str, collection_uuid: str) -> List[str]:
    keys: List[str] = []
    continuation_token: Optional[str] = None
    while True:
        query = {"list-type": "2", "max-keys": "1000"}
        if continuation_token is not None:
            query["continuation-token"] = continuation_token

        body = _minio_signed_get(bucket, query)
        root = ET.fromstring(body)

        for element in root.iter():
            if _xml_local_name(element.tag) != "Contents":
                continue
            key = _child_text(element, "Key")
            if (
                key is not None
                and collection_uuid in key
                and not _is_collection_version_file_key(key, collection_uuid)
            ):
                keys.append(key)

        is_truncated = _child_text(root, "IsTruncated") == "true"
        if not is_truncated:
            break
        continuation_token = _child_text(root, "NextContinuationToken")
        if continuation_token is None:
            pytest.fail(
                "MinIO returned a truncated object listing without a continuation token"
            )

    return sorted(keys)


def _format_minio_file_sample(paths: List[str]) -> str:
    sample_size = 50
    sample = paths[:sample_size]
    suffix = (
        "" if len(paths) <= sample_size else f" ... and {len(paths) - sample_size} more"
    )
    return f"{sample}{suffix}"


def _wait_for_minio_files_for_collection(collection_uuid: str) -> Dict[str, List[str]]:
    deadline = time.monotonic() + MINIO_OBJECT_APPEAR_TIMEOUT_SECONDS
    paths_by_bucket: Dict[str, List[str]] = {}
    while True:
        paths_by_bucket = {
            bucket: _list_minio_files_for_collection(bucket, collection_uuid)
            for bucket in MINIO_BUCKETS
        }
        if all(paths_by_bucket.values()):
            return paths_by_bucket
        if time.monotonic() >= deadline:
            missing_buckets = [
                bucket for bucket, paths in paths_by_bucket.items() if not paths
            ]
            pytest.fail(
                "Expected MinIO to contain non-version files for collection "
                f"{collection_uuid} in every test bucket before deletion, "
                f"but these buckets had none: {missing_buckets}. "
                f"Found counts: "
                f"{ {bucket: len(paths) for bucket, paths in paths_by_bucket.items()} }"
            )
        time.sleep(1)


def _wait_for_minio_files_deleted(collection_uuid: str) -> None:
    deadline = time.monotonic() + MINIO_OBJECT_DELETE_TIMEOUT_SECONDS
    paths_by_bucket: Dict[str, List[str]] = {}
    while True:
        paths_by_bucket = {
            bucket: _list_minio_files_for_collection(bucket, collection_uuid)
            for bucket in MINIO_BUCKETS
        }
        if not any(paths_by_bucket.values()):
            return
        if time.monotonic() >= deadline:
            remaining = {
                bucket: paths for bucket, paths in paths_by_bucket.items() if paths
            }
            samples = {
                bucket: _format_minio_file_sample(paths)
                for bucket, paths in remaining.items()
            }
            pytest.fail(
                "Expected non-version MinIO files for collection "
                f"{collection_uuid} to be deleted from every test bucket, "
                f"but found files in these buckets: "
                f"{ {bucket: len(paths) for bucket, paths in remaining.items()} }. "
                f"Samples: {samples}"
            )
        time.sleep(1)


def round_robin(round_index, round_around):
    return round_around[round_index % len(round_around)]


def _delete_collection_and_assert_gc_hard_delete(
    client: ClientAPI, collection_name: str, collection_uuid: str
) -> None:
    watchers = _start_gc_log_watchers()
    captured_stdout = {namespace: [] for namespace in GC_NAMESPACES}
    captured_stderr: Dict[str, str] = {}
    try:
        client.delete_collection(collection_name)
        _wait_for_gc_hard_delete_log(watchers, captured_stdout, collection_uuid)
    finally:
        captured_stderr = _stop_gc_log_watchers(watchers, captured_stdout)

    stdout_by_namespace = {
        namespace: "".join(lines) for namespace, lines in captured_stdout.items()
    }
    matching_namespaces = [
        namespace
        for namespace, stdout in stdout_by_namespace.items()
        if _hard_delete_log_found(stdout, collection_uuid)
    ]

    for namespace, stdout in stdout_by_namespace.items():
        print(f"{namespace} GC stdout captured {len(stdout)} bytes")
    for namespace, stderr in captured_stderr.items():
        if stderr:
            print(f"{namespace} GC stderr: {stderr[-1000:]}")

    assert matching_namespaces, (
        "Expected garbage collector logs to hard delete collection "
        f"{collection_uuid}. Captured stdout tails: "
        f"{ {namespace: stdout[-2000:] for namespace, stdout in stdout_by_namespace.items()} }"
    )


@pytest.mark.skipif(
    not MULTI_REGION_ENABLED,
    reason="MCMR GC coverage requires a multi-region Kubernetes cluster",
)
def test_add_gc_hard_deletes_empty_mcmr_collection() -> None:
    client1, client2 = _create_mcmr_clients()
    _create_isolated_database_mcmr(client1, client2, "tilt-spanning")

    collection_name = f"test_add_gc_empty_{uuid.uuid4().hex}"
    collection = client1.create_collection(name=collection_name)
    client2.get_collection(name=collection_name)

    _delete_collection_and_assert_gc_hard_delete(
        client1, collection_name, str(collection.id)
    )


@pytest.mark.skipif(
    not MULTI_REGION_ENABLED,
    reason="MCMR GC coverage requires a multi-region Kubernetes cluster",
)
@pytest.mark.skip_single_region
def test_add_gc_hard_deletes_mcmr_collection() -> None:
    client1, client2 = _create_mcmr_clients()
    _create_isolated_database_mcmr(client1, client2, "tilt-spanning")
    clients = [client1, client2]

    collection_name = f"test_add_gc_{uuid.uuid4().hex}"
    coll1 = client1.create_collection(name=collection_name)
    coll2 = client2.get_collection(name=collection_name)
    collection_uuid = str(coll1.id)
    collections = [coll1, coll2]

    current_version1 = cast(int, coll1.get_model()["version"])
    current_version2 = cast(int, coll2.get_model()["version"])

    for round_index in range(COMPACTION_ROUNDS):
        writer_client = round_robin(round_index, clients)
        writer_collection = round_robin(round_index, collections)
        _add_round(writer_client, writer_collection, round_index)

        current_version1 = wait_for_version_increase(
            client1, collection_name, current_version1
        )
        current_version2 = wait_for_version_increase(
            client2, collection_name, current_version2
        )

    minio_files_before_delete_by_bucket = _wait_for_minio_files_for_collection(
        collection_uuid
    )
    for bucket, paths in minio_files_before_delete_by_bucket.items():
        print(
            f"MinIO bucket {bucket} contained {len(paths)} files for collection "
            f"{collection_uuid} before deletion"
        )

    _delete_collection_and_assert_gc_hard_delete(
        client1, collection_name, collection_uuid
    )
    _wait_for_minio_files_deleted(collection_uuid)
