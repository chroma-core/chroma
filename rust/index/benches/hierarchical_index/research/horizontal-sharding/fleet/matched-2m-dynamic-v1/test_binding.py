import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

loader=importlib.util.spec_from_file_location('binding',Path(__file__).with_name('bind-restored-input.py'))
binding=importlib.util.module_from_spec(loader);loader.loader.exec_module(binding)

class BindingTests(unittest.TestCase):
    def test_changed_payload_rejects(self):
        with tempfile.TemporaryDirectory() as t:
            p=Path(t)/'data';p.write_bytes(b'old')
            d=dict(path=str(p),bytes=3,sha256=hashlib.sha256(b'old').hexdigest());p.write_bytes(b'new')
            with self.assertRaises(ValueError):binding.verify(d)

    def scenario(self, change=None):
        with tempfile.TemporaryDirectory() as t:
            r=Path(t)
            def save(name,value):
                p=r/name;p.write_text(json.dumps(value));b=p.read_bytes()
                return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
            budget=dict(deadline_unix=time.time()+60,min_free_bytes=100)
            restore=save('spec',budget)
            canonical=dict(path=str(r/'canonical'),bytes=6144000000,sha256='0'*64)
            receipt=dict(budget,status='complete',plan_sha256=binding.PLAN,rows=2000000,dimension=768,expected_bytes=6144000000,canonical=canonical)
            runtime=dict(receipt,materializer_sha256=binding.MATERIALIZER,spec_sha256=restore['sha256'],aws_identity={'Account':'079819354890'})
            query=save('queries',{'fixture':'synthetic'})
            base=dict(budget)
            if change:change(base,receipt,runtime)
            spec=dict(base=save('base',base),restore_receipt=save('receipt',receipt),restore_runtime=save('runtime',runtime),restore_spec=restore,matched_inputs=save('inputs',dict(queries=query,config={'fixture':True})),queries=query,output=str(r/'out'))
            real_verify=binding.verify
            # Mock only the 6.144GB canonical read; small metadata/payload hashes stay real.
            def verify(d):return Path(d['path']) if d['path']==canonical['path'] else real_verify(d)
            with patch.object(binding,'verify',side_effect=verify):binding.create(spec)
            self.assertTrue((r/'out/native-common.json').is_file())

    def test_success_emits_same_canonical_binding(self):self.scenario()
    def test_deadline_mismatch(self):
        with self.assertRaisesRegex(ValueError,'budget differs'):self.scenario(lambda b,r,v:b.update(deadline_unix=0))
    def test_wrong_producer(self):
        with self.assertRaisesRegex(ValueError,'producer differs'):self.scenario(lambda b,r,v:v.update(materializer_sha256='bad'))
    def test_wrong_payload_extent(self):
        with self.assertRaisesRegex(ValueError,'extent differs'):self.scenario(lambda b,r,v:r['canonical'].update(bytes=1))
    def test_receipt_disagreement(self):
        with self.assertRaisesRegex(ValueError,'disagreement'):self.scenario(lambda b,r,v:v.update(status='failed'))

if __name__=='__main__':unittest.main()
