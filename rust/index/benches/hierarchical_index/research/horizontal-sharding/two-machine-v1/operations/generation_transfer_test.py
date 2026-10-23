"""Local dependency-order and byte-integrity tests; no AWS calls."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("generation_transfer", Path(__file__).with_name("generation_transfer.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Body(io.BytesIO):
    def iter_chunks(self, chunk_size):
        while True:
            block = self.read(chunk_size)
            if not block:
                break
            yield block


class PreconditionFailed(Exception):
    response={"Error":{"Code":"PreconditionFailed"},"ResponseMetadata":{"HTTPStatusCode":412}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.order = []

    def get_bucket_versioning(self, **args):
        raise AssertionError("task role does not authorize GetBucketVersioning")

    def put_object(self, **args):
        assert args["IfNoneMatch"] == "*"
        assert args["Bucket"] == m.BUCKET
        assert args["ExpectedBucketOwner"] == m.ACCOUNT
        key = args["Key"]
        if key in self.objects:
            raise PreconditionFailed("immutable key exists")
        data = args["Body"].read()
        assert len(data) == args["ContentLength"]
        assert args["ChecksumSHA256"] == m.base64.b64encode(m.hashlib.sha256(data).digest()).decode()
        version = "v" + str(len(self.objects) + 1)
        self.objects[key] = (version, data, args["Metadata"])
        self.order.append(key)
        return {"VersionId": version}

    def head_object(self, **args):
        assert args["ExpectedBucketOwner"] == m.ACCOUNT
        version, data, metadata = self.objects[args["Key"]]
        if "VersionId" in args:
            assert args["VersionId"] == version
        return {"VersionId": version, "ContentLength": len(data), "Metadata": metadata}

    def get_object(self, **args):
        head = self.head_object(**args)
        head["Body"] = Body(self.objects[args["Key"]][1])
        return head


def fixture(root):
    def write(name, body):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        measured = m.digest(path)
        return {"path": name, "bytes": measured["bytes"], "checksum": measured["checksum"]}
    routing = write("objects/routing", b"opaque routing")
    posting = write("objects/postings", b"actual binary posting bytes")
    vectors = write("objects/vector-descriptor", b"pinned corpus descriptor, no corpus bytes")
    fmt = {"version": 1, "dimensions": 17, "metric": "l2", "centroid_bits": 4, "posting_bits": 1, "vector_bits": 32}
    owner = {"run": "r", "save_attempt": "s", "generation": "g", "owner": 1, "roots": [123], "vector_count": 2, "format": fmt, "nodes": [routing], "postings": [posting], "full_vectors": [vectors]}
    owner_ref = write("owners/owner.json", json.dumps(owner).encode())
    manifest = {"run": "r", "save_attempt": "s", "generation": "g", "format": fmt, "expected_roots": [{"root": 123, "owner": 1}], "owners": [owner_ref], "shared_routing": routing, "full_vectors": [vectors], "total_vectors": 2}
    manifest_ref = write("generations/g.json", json.dumps(manifest).encode())
    write("descriptor.json", json.dumps({"generation": "g", "save_attempt": "s", "manifest": manifest_ref}).encode())
    return root / "descriptor.json"


class TransferTests(unittest.TestCase):
    def test_manifest_last_exact_versions_restore_and_idempotent_resume(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "writer"
            descriptor = fixture(root)
            receipt_path = Path(directory) / "receipt.json"
            s3 = FakeS3()
            receipt = m.upload(s3, {"account": m.ACCOUNT}, root, descriptor, "two-machine-v1/frozen-test", receipt_path, 1024**2, 1024**2)
            self.assertTrue(receipt["published"])
            self.assertEqual(receipt["format"],2)
            self.assertEqual(receipt["key_layout"],"sha256-v1")
            self.assertEqual(receipt["objects"][-1]["role"],"descriptor")
            self.assertEqual(receipt["objects"][-2]["role"],"generation_manifest")
            self.assertEqual(s3.order[-1],m.content_key(receipt["objects"][-1]["sha256"]))
            self.assertTrue(all(e["key"]==m.content_key(e["sha256"]) for e in receipt["objects"]))
            before = copy.deepcopy(s3.order)
            m.upload(s3, {"account": m.ACCOUNT}, root, descriptor, "two-machine-v1/frozen-test", receipt_path, 1024**2, 1024**2)
            self.assertEqual(before, s3.order)
            destination = Path(directory) / "fresh-reader"
            self.assertEqual(m.restore(s3, receipt_path, destination, 1024**2, 1024**2)["generation"], "g")
            self.assertEqual((destination / "objects/postings").read_bytes(), (root / "objects/postings").read_bytes())
            with self.assertRaises(ValueError):
                m.restore(s3, receipt_path, destination, 1024**2, 1024**2)

    def test_bad_reference_bounds_and_restore_corruption_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "writer"
            descriptor = fixture(root)
            with self.assertRaises(ValueError):
                m.plan(root, descriptor, 1, 1024**2)
            receipt_path = Path(directory) / "receipt.json"
            s3 = FakeS3()
            receipt = m.upload(s3, {}, root, descriptor, "two-machine-v1/bad-test", receipt_path, 1024**2, 1024**2)
            key = receipt["objects"][0]["key"]
            version, data, metadata = s3.objects[key]
            s3.objects[key] = (version, b"x" * len(data), metadata)
            with self.assertRaises(ValueError):
                m.restore(s3, receipt_path, Path(directory) / "corrupt-reader", 1024**2, 1024**2)
            for value in ("../escape", "/absolute", "foo/../bar", "foo//bar"):
                with self.assertRaises(ValueError):
                    m.relative(value)
            (root / "objects/postings").write_bytes(b"mutated")
            with self.assertRaises(ValueError):
                m.plan(root, descriptor, 1024**2, 1024**2)

    def test_content_reuse_reads_actual_bytes_and_rejects_conflict(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"writer"
            descriptor=fixture(root)
            s3=FakeS3()
            first=m.upload(s3,{},root,descriptor,"two-machine-v1/first",Path(directory)/"one.json",1024**2,1024**2)
            before=list(s3.order)
            second=m.upload(s3,{},root,descriptor,"two-machine-v1/second",Path(directory)/"two.json",1024**2,1024**2)
            self.assertTrue(second["published"])
            self.assertEqual(before,s3.order)
            self.assertTrue(all(e["reused_existing"] for e in second["objects"]))
            self.assertEqual([e["version_id"] for e in first["objects"]],[e["version_id"] for e in second["objects"]])
            bad=second["objects"][0]
            version,data,metadata=s3.objects[bad["key"]]
            s3.objects[bad["key"]]=(version,b"x"*len(data),metadata)
            with self.assertRaises(ValueError):
                m.upload(s3,{},root,descriptor,"two-machine-v1/conflict",Path(directory)/"bad.json",1024**2,1024**2)
            self.assertFalse(json.loads((Path(directory)/"bad.json").read_text())["published"])
            self.assertEqual(before,s3.order)

    def test_restore_rejects_legacy_or_wrong_content_key_layout(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/"writer"
            descriptor=fixture(root)
            s3=FakeS3()
            receipt_path=Path(directory)/"receipt.json"
            receipt=m.upload(s3,{},root,descriptor,"two-machine-v1/strict",receipt_path,1024**2,1024**2)
            receipt["objects"][0]["key"]="two-machine-v1/not-authorized-layout"
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                m.restore(s3,receipt_path,Path(directory)/"reader",1024**2,1024**2)
            receipt["format"]=1
            receipt_path.write_text(json.dumps(receipt))
            with self.assertRaises(ValueError):
                m.restore(s3,receipt_path,Path(directory)/"reader2",1024**2,1024**2)


if __name__ == "__main__":
    unittest.main()
