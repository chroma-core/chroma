import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from admission import admit, verify_file

HERE = Path(__file__).resolve().parent

class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.spec = json.loads((HERE/'runtime-spec.proposed.json').read_text())
        self.caps = dict(binary_sha256=self.spec['binary_sha256'], fp_row_cap=2000000,
                         stage_row_cap=2000000, fp_node_cap=4096, fp_payload_cap_bytes=16*1024**2,
                         manifest_schema=self.spec['manifest_schema'], trace_kind='bounded-gt-intersections-v1')
        self.shape = dict(nodes=1780, internal_nodes=28, leaves=1752, depth=3)
    def test_retained_binary_is_rejected_before_launch(self):
        with self.assertRaisesRegex(ValueError, 'cap'):
            admit(self.spec, json.loads((HERE/'retained-capabilities.json').read_text()), self.shape)
    def test_bounded_future_contract_does_not_claim_internal_pruning(self):
        self.assertFalse(admit(self.spec, self.caps, self.shape)['internal_pruning_claim'])
    def test_each_cap_binding_schema_and_shape_failure(self):
        for key, value in [('fp_node_cap',256), ('fp_payload_cap_bytes',1048576), ('fp_row_cap',16384),
                           ('stage_row_cap',16384), ('manifest_schema','old'), ('trace_kind','full-ids'), ('binary_sha256','bad')]:
            caps=copy.deepcopy(self.caps);caps[key]=value
            with self.subTest(key=key), self.assertRaises(ValueError):admit(self.spec,caps,self.shape)
        for depth in [None,0,17]:
            shape=dict(self.shape,depth=depth)
            with self.assertRaises((ValueError,TypeError)):admit(self.spec,self.caps,shape)
    def test_integrity_rejects_changed_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'x').write_bytes(b'abc');pin=dict(path='x',bytes=3,sha256=hashlib.sha256(b'abc').hexdigest())
            self.assertEqual(verify_file(root,pin),root/'x');(root/'x').write_bytes(b'abd')
            with self.assertRaisesRegex(ValueError,'integrity'):verify_file(root,pin)
if __name__=='__main__':unittest.main()

class ReplyTests(unittest.TestCase):
    def test_bounded_mock_reply_and_rejection(self):
        from admission import validate_summary
        ids=list(range(100));r=dict(query_id='q0',snapshot_sha256='s',gt=dict(complete=True,ground_truth_count=100,reached_ids=ids[:98],retained_ids=ids[:96],returned_ids=ids[:96],navigation_missing=2,trim_missing=2,exact_missing=0),ids=ids[:96]+list(range(100,104)),distance_bits=[0]*100,work=dict(navigation_level_trace=dict(complete=True,levels=[])),selected_leaves=64,posting_rows=65000)
        self.assertEqual(validate_summary(r,'q0',ids,'s'),dict(navigation_loss=2,trim_loss=2,exact_loss=0))
        for key,value in [('query_id','wrong'),('snapshot_sha256','wrong'),('selected_leaves',4097),('posting_rows',4000001)]:
            with self.subTest(key=key),self.assertRaises(ValueError):validate_summary(dict(r,**{key:value}),'q0',ids,'s')
        bad=copy.deepcopy(r);bad['gt']['reached_ids']=[999]
        with self.assertRaises(ValueError):validate_summary(bad,'q0',ids,'s')
