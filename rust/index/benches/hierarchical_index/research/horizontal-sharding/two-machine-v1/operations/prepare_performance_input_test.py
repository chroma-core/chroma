"""Local plan validation and small mocked byte-stream tests; no AWS calls."""
import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location("prepare_input",Path(__file__).with_name("prepare-performance-input.py"))
m=importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)

class FakeS3:
    def __init__(self,objects):
        self.objects=objects
        self.calls=[]
    def get_object(self,**args):
        self.calls.append(args)
        version,data=self.objects[args["Key"]]
        assert args["VersionId"]==version
        assert args["ExpectedBucketOwner"]==m.ACCOUNT
        return {"VersionId":version,"ContentLength":len(data),"Body":io.BytesIO(data)}

class InputTests(unittest.TestCase):
    def test_reviewed_real_plan_and_invalid_source_changes(self):
        path=Path(__file__).parents[2]/"single-shard-3m-attribution-v1/operations/input-plan.json"
        plan=json.loads(path.read_text())
        m.validate_plan(plan)
        for mutation in (lambda p:p["chunks"][1].update(start_id=100001),lambda p:p["chunks"][0]["artifact"].update(version_id="null"),lambda p:p["chunks"][0]["artifact"].update(bucket="other-account")):
            changed=copy.deepcopy(plan)
            mutation(changed)
            with self.assertRaises(ValueError):m.validate_plan(changed)

    def test_one_download_per_chunk_and_100k_prefix_tee(self):
        first=bytes(range(16));second=bytes(range(16,40));full=first+second[:16]
        chunks=[]
        for start,data,key in [(0,first,"first"),(2,second,"second")]:
            chunks.append({"start_id":start,"take_rows":2,"artifact":{"bucket":m.BUCKET,"key":key,"version_id":"pinned","bytes":len(data),"sha256":hashlib.sha256(data).hexdigest()}})
        plan={"chunks":chunks}
        with tempfile.TemporaryDirectory() as directory,patch.multiple(m,ROWS=4,DIM=2,WIDTH=8,TOTAL_BYTES=32,FIRST_ROWS=2,FIRST_SHA=hashlib.sha256(first).hexdigest(),TOTAL_SHA=hashlib.sha256(full).hexdigest()):
            root=Path(directory);passed=root/"passed";passed.mkdir();s3=FakeS3({"first":("pinned",first),"second":("pinned",second)})
            report=m.materialize(s3,plan,passed,time.time()+100)
            self.assertEqual(len(s3.calls),2)
            self.assertEqual(report["network_payload_bytes"],40)
            self.assertEqual(report["bytes"],32)
            self.assertEqual(report["first100K_additional_downloads"],0)
            self.assertEqual((passed/"fineweb100k.f32").read_bytes(),first)
            self.assertEqual((passed/"fineweb3m.f32").read_bytes(),full)
            self.assertEqual(json.loads((passed/"100k.json").read_text())["rows"],2)
            self.assertEqual(json.loads((passed/"3m.json").read_text())["sha256"],hashlib.sha256(full).hexdigest())
            failed=root/"failed";failed.mkdir()
            with patch.object(m,"TOTAL_SHA","0"*64),self.assertRaises(ValueError):
                m.materialize(FakeS3({"first":("pinned",first),"second":("pinned",second)}),plan,failed,time.time()+100)
            self.assertTrue((failed/"failure.json").exists())
            self.assertTrue((failed/"fineweb3m.f32.incomplete").exists())
            self.assertFalse((failed/"fineweb3m.f32").exists())
            self.assertFalse((failed/"3m.json").exists())

if __name__=="__main__":unittest.main()
