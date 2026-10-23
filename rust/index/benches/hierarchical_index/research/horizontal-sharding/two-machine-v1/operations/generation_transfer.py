#!/usr/bin/env python3
"""Transfer one frozen SPANN generation, preserving exact S3 versions.

This tool never transfers corpus bytes. Run from the verified sandbox task host
with the operator's Cops-managed AWS environment and a fresh shutdown window.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import time

ACCOUNT = "079819354890"
REGION = "us-east-1"
BUCKET = "spann-artifacts-079819354890-20261014-ecfcca4d"
CHUNK = 1024 * 1024
MASK64 = (1 << 64) - 1


def check_deadline(deadline):
    if deadline is not None and time.time() >= deadline:
        raise TimeoutError("absolute generation-transfer deadline reached")


def relative(value):
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError("invalid relative object path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(p in ("", ".", "..") for p in value.split("/")):
        raise ValueError("object path must be relative without traversal")
    return value


def digest(path):
    sha = hashlib.sha256()
    fnv, size = 0xCBF29CE484222325, 0
    with open(path, "rb") as file:
        while True:
            block = file.read(CHUNK)
            if not block:
                break
            sha.update(block)
            size += len(block)
            for byte in block:
                fnv = ((fnv ^ byte) * 0x100000001B3) & MASK64
    return {"bytes": size, "sha256": sha.hexdigest(), "checksum": fnv}


def checked_path(root, name):
    relative(name)
    result = root / name
    if result.is_symlink() or not result.resolve().is_relative_to(root.resolve()):
        raise ValueError("object escapes saved namespace")
    return result


def verify_ref(root, ref):
    measured = digest(checked_path(root, ref["path"]))
    if measured["bytes"] != ref["bytes"] or measured["checksum"] != ref["checksum"]:
        raise ValueError("saved object integrity mismatch: " + ref["path"])
    return measured


def json_ref(root, ref):
    verify_ref(root, ref)
    with open(checked_path(root, ref["path"]), "rb") as file:
        return json.load(file)


def plan(root, descriptor_path, max_object_bytes, max_total_bytes):
    root = Path(root).resolve()
    descriptor_bytes = Path(descriptor_path).read_bytes()
    descriptor = json.loads(descriptor_bytes)
    manifest = json_ref(root, descriptor["manifest"])
    if any(descriptor[k] != manifest[k] for k in ("generation", "save_attempt")):
        raise ValueError("descriptor and generation identity differ")
    fmt = manifest["format"]
    if (fmt["version"], fmt["centroid_bits"], fmt["posting_bits"], fmt["vector_bits"]) != (1, 4, 1, 32) or fmt["dimensions"] <= 0 or fmt["metric"] not in ("l2", "cosine", "ip"):
        raise ValueError("unsupported generation format")
    payloads, owner_refs, actual, owner_ids, vectors = {}, [], {}, set(), {}

    def payload(ref):
        if not ref["path"].startswith("objects/"):
            raise ValueError("payload is outside the immutable object namespace")
        previous = payloads.setdefault(ref["path"], ref)
        if previous != ref:
            raise ValueError("conflicting references to one saved object")

    payload(manifest["shared_routing"])
    total_vectors = 0
    for ref in manifest["owners"]:
        if not ref["path"].startswith("owners/"):
            raise ValueError("owner manifest is outside its namespace")
        owner = json_ref(root, ref)
        if any(owner[k] != manifest[k] for k in ("run", "save_attempt", "generation", "format")) or owner["owner"] in owner_ids:
            raise ValueError("owner save identity, format or owner coverage differs")
        owner_ids.add(owner["owner"])
        for node in owner["roots"]:
            if node in actual:
                raise ValueError("root covered more than once")
            actual[node] = owner["owner"]
        total_vectors += owner["vector_count"]
        for item in owner["nodes"] + owner["postings"] + owner["full_vectors"]:
            payload(item)
        for item in owner["full_vectors"]:
            old = vectors.setdefault(item["path"], item)
            if old != item:
                raise ValueError("incompatible full-vector views")
        owner_refs.append(ref)
    expected = {}
    for item in manifest["expected_roots"]:
        if item["root"] in expected:
            raise ValueError("duplicate expected root")
        expected[item["root"]] = item["owner"]
    full_vectors = {ref["path"]: ref for ref in manifest["full_vectors"]}
    if not actual or actual != expected or total_vectors != manifest["total_vectors"] or vectors != full_vectors:
        raise ValueError("incomplete roots, counts or full-vector reference coverage")
    entries = []
    for role, refs in (("payload", sorted(payloads.values(), key=lambda r: r["path"])), ("owner_manifest", owner_refs), ("generation_manifest", [descriptor["manifest"]])):
        for ref in refs:
            measured = verify_ref(root, ref)
            entries.append({"role": role, "relative_path": ref["path"], **measured})
    descriptor_digest = hashlib.sha256(descriptor_bytes).hexdigest()
    # The descriptor has no self-reference; compute its independent integrity.
    with tempfile.NamedTemporaryFile() as file:
        file.write(descriptor_bytes)
        file.flush()
        measured = digest(file.name)
    assert measured["sha256"] == descriptor_digest
    entries.append({"role": "descriptor", "relative_path": "descriptor.json", **measured})
    if len({e["relative_path"] for e in entries}) != len(entries):
        raise ValueError("duplicate publication path")
    if any(e["bytes"] > max_object_bytes for e in entries) or sum(e["bytes"] for e in entries) > max_total_bytes:
        raise ValueError("generation exceeds explicit transfer byte limits")
    return descriptor, descriptor_bytes, entries


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_json(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as file:
            json.dump(body, file, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def client(profile):
    import boto3
    from botocore.config import Config
    session = boto3.Session(profile_name=profile, region_name=REGION)
    identity = session.client("sts", region_name=REGION).get_caller_identity()
    if identity["Account"] != ACCOUNT:
        raise ValueError("AWS identity is outside the authorized sandbox")
    return session.client("s3", region_name=REGION, config=Config(connect_timeout=15, read_timeout=30, retries={"max_attempts": 2})), {"account": identity["Account"], "arn": identity["Arn"]}


def content_key(sha256):
    if not re.fullmatch(r"[0-9a-f]{64}",sha256):
        raise ValueError("invalid content-addressed object digest")
    return "sha256/"+sha256[:2]+"/"+sha256


def existing_content(s3,key,entry,deadline):
    response=s3.get_object(Bucket=BUCKET,Key=key,ExpectedBucketOwner=ACCOUNT)
    try:
        version=response.get("VersionId")
        if not version or version=="null" or response["ContentLength"]!=entry["bytes"]:
            raise ValueError("existing immutable object version or length differs")
        digest=hashlib.sha256();received=0
        for block in response["Body"].iter_chunks(chunk_size=CHUNK):
            check_deadline(deadline)
            received+=len(block)
            if received>entry["bytes"]:
                raise ValueError("existing immutable object exceeded byte bound")
            digest.update(block)
        if received!=entry["bytes"] or digest.hexdigest()!=entry["sha256"]:
            raise ValueError("existing immutable object bytes conflict with content key")
        return version
    finally:
        response["Body"].close()


def check_head(s3, entry):
    result = s3.head_object(Bucket=BUCKET, Key=entry["key"], VersionId=entry["version_id"], ExpectedBucketOwner=ACCOUNT)
    if result["ContentLength"] != entry["bytes"] or result.get("Metadata", {}).get("sha256",entry["sha256"]) != entry["sha256"] or result.get("VersionId") != entry["version_id"]:
        raise ValueError("uploaded object receipt differs from its exact version")


def upload(s3, identity, root, descriptor_path, prefix, receipt_path, max_object_bytes, max_total_bytes, deadline=None):
    check_deadline(deadline)
    prefix = relative(prefix.rstrip("/"))
    descriptor, descriptor_bytes, entries = plan(root, descriptor_path, max_object_bytes, max_total_bytes)
    receipt_path = Path(receipt_path)
    receipt = {"format": 2, "key_layout":"sha256-v1", "bucket": BUCKET, "account": ACCOUNT, "region": REGION, "prefix": prefix, "generation": descriptor["generation"], "save_attempt": descriptor["save_attempt"], "identity": identity, "published": False, "objects": []}
    if receipt_path.exists():
        saved = json.loads(receipt_path.read_text())
        for key in ("format", "key_layout", "bucket", "account", "region", "prefix", "generation", "save_attempt"):
            if saved.get(key) != receipt[key]:
                raise ValueError("existing receipt belongs to another publication")
        receipt = saved
    existing = {entry["relative_path"]: entry for entry in receipt["objects"]}
    if len(existing) != len(receipt["objects"]) or set(existing) - {e["relative_path"] for e in entries}:
        raise ValueError("receipt contains unexpected publication objects")
    atomic_json(receipt_path, receipt)
    for entry in entries:
        check_deadline(deadline)
        key = content_key(entry["sha256"])
        if entry["relative_path"] in existing:
            old = existing[entry["relative_path"]]
            if any(old.get(k) != v for k, v in entry.items()) or old["key"] != key:
                raise ValueError("resumed save bytes differ from frozen receipt")
            check_head(s3, old)
            continue
        path = Path(descriptor_path) if entry["role"] == "descriptor" else checked_path(Path(root), entry["relative_path"])
        measured = digest(path)
        if any(measured[k] != entry[k] for k in measured):
            raise ValueError("saved file changed during export")
        reused_existing=False
        try:
            with open(path, "rb") as body:
                response = s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentLength=entry["bytes"], IfNoneMatch="*", ExpectedBucketOwner=ACCOUNT, ChecksumSHA256=base64.b64encode(bytes.fromhex(entry["sha256"])).decode(), Metadata={"sha256": entry["sha256"]})
            version=response.get("VersionId")
        except Exception as error:
            response=getattr(error,"response",{})
            if str(response.get("Error",{}).get("Code")) not in ("412","PreconditionFailed") and response.get("ResponseMetadata",{}).get("HTTPStatusCode")!=412:
                raise
            # Reuse only after bounded readback proves the actual bytes and
            # returns their exact version. Never replace a conflicting object.
            version=existing_content(s3,key,entry,deadline)
            reused_existing=True
        if not version or version == "null":
            raise ValueError("upload returned no durable version ID")
        saved = {**entry, "key": key, "version_id": version, "reused_existing":reused_existing}
        check_head(s3, saved)
        receipt["objects"].append(saved)
        atomic_json(receipt_path, receipt)
    receipt["published"] = True
    atomic_json(receipt_path, receipt)
    return receipt


def restore(s3, receipt_path, destination, max_object_bytes, max_total_bytes, deadline=None):
    check_deadline(deadline)
    receipt = json.loads(Path(receipt_path).read_text())
    if any(receipt.get(k) != v for k, v in (("format", 2), ("key_layout","sha256-v1"), ("bucket", BUCKET), ("account", ACCOUNT), ("region", REGION), ("published", True))):
        raise ValueError("receipt does not confirm an authorized published generation")
    prefix = relative(receipt["prefix"])
    entries = receipt["objects"]
    roles = {"payload": 0, "owner_manifest": 1, "generation_manifest": 2, "descriptor": 3}
    if not entries or entries[-1]["role"] != "descriptor" or sum(e["role"] == "descriptor" for e in entries) != 1 or sum(e["role"] == "generation_manifest" for e in entries) != 1:
        raise ValueError("receipt does not have one manifest and descriptor last")
    if any(roles[a["role"]] > roles[b["role"]] for a, b in zip(entries, entries[1:])):
        raise ValueError("publication dependency order is invalid")
    seen = set()
    for entry in entries:
        name = relative(entry["relative_path"])
        if name in seen or entry["key"] != content_key(entry["sha256"]) or not entry.get("version_id") or entry["version_id"] == "null" or not re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) or not 0 <= entry["bytes"] <= max_object_bytes:
            raise ValueError("invalid exact-version object receipt")
        seen.add(name)
    if sum(e["bytes"] for e in entries) > max_total_bytes:
        raise ValueError("restore exceeds total transfer limit")
    destination = Path(destination)
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("restore requires a new or empty reader namespace")
    destination.mkdir(parents=True, exist_ok=True)
    for entry in entries:
        check_deadline(deadline)
        path = checked_path(destination, entry["relative_path"])
        path.parent.mkdir(parents=True, exist_ok=True)
        response = s3.get_object(Bucket=BUCKET, Key=entry["key"], VersionId=entry["version_id"], ExpectedBucketOwner=ACCOUNT)
        if response.get("VersionId") != entry["version_id"] or response["ContentLength"] != entry["bytes"]:
            raise ValueError("download version or length differs from receipt")
        temporary = path.with_name(path.name + ".partial")
        received = 0
        try:
            with open(temporary, "xb") as file:
                for block in response["Body"].iter_chunks(chunk_size=CHUNK):
                    check_deadline(deadline)
                    received += len(block)
                    if received > entry["bytes"]:
                        raise ValueError("object exceeded declared length")
                    file.write(block)
                file.flush()
                os.fsync(file.fileno())
        finally:
            response["Body"].close()
        if digest(temporary) != {k: entry[k] for k in ("bytes", "sha256", "checksum")}:
            raise ValueError("restored bytes fail SHA-256 or saved reference checksum")
        os.replace(temporary, path)
        sync_directory(path.parent)
    # Parse the restored dependency graph as well as checking every byte receipt.
    descriptor, _, _ = plan(destination, destination / "descriptor.json", max_object_bytes, max_total_bytes)
    if descriptor["generation"] != receipt["generation"] or descriptor["save_attempt"] != receipt["save_attempt"]:
        raise ValueError("restored descriptor differs from publication identity")
    return descriptor


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "upload", "restore"))
    parser.add_argument("--root", type=Path)
    parser.add_argument("--descriptor", type=Path)
    parser.add_argument("--prefix", help="logical campaign label; S3 keys use sha256/<first2>/<digest>")
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--destination", type=Path)
    parser.add_argument("--profile")
    parser.add_argument("--shutdown-at-unix", type=int)
    parser.add_argument("--max-object-bytes", type=int, default=1024**3)
    parser.add_argument("--max-total-bytes", type=int, default=2 * 1024**3)
    args = parser.parse_args()
    if args.max_object_bytes <= 0 or args.max_total_bytes <= 0:
        parser.error("positive transfer limits are required")
    if args.mode in ("plan", "upload") and (args.root is None or args.descriptor is None):
        parser.error("--root and --descriptor are required")
    if args.mode == "plan":
        descriptor, _, entries = plan(args.root, args.descriptor, args.max_object_bytes, args.max_total_bytes)
        print(json.dumps({"generation": descriptor["generation"], "objects": len(entries), "bytes": sum(e["bytes"] for e in entries), "order": entries}, indent=2))
        return
    if args.receipt is None:
        parser.error("--receipt is required for cloud transfer")
    if args.mode == "upload" and not args.prefix:
        parser.error("--prefix is required for upload")
    if args.mode == "restore" and args.destination is None:
        parser.error("--destination is required for restore")
    if args.shutdown_at_unix is None or not time.time() < args.shutdown_at_unix <= time.time() + 5400:
        parser.error("cloud transfer requires a fresh --shutdown-at-unix within 90 minutes")
    check_deadline(args.shutdown_at_unix)
    s3, identity = client(args.profile)
    if args.mode == "upload":
        result = upload(s3, identity, args.root, args.descriptor, args.prefix, args.receipt, args.max_object_bytes, args.max_total_bytes, args.shutdown_at_unix)
        print(json.dumps({"published": result["published"], "receipt": str(args.receipt), "objects": len(result["objects"]), "bytes": sum(e["bytes"] for e in result["objects"])}))
    else:
        result = restore(s3, args.receipt, args.destination, args.max_object_bytes, args.max_total_bytes, args.shutdown_at_unix)
        print(json.dumps({"restored_generation": result["generation"], "destination": str(args.destination), "identity": identity}))


if __name__ == "__main__":
    main()
