import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from manifest_bridge import bridge, EXPECTED_CONFIG

class BridgeTests(unittest.TestCase):
    def fixture(self,root):
        (root/'store').mkdir();(root/'store/data').mkdir();data=b''.join(i.to_bytes(8,'little') for i in range(3));(root/'store/map').write_bytes(data)
        explicit={k:v for k,v in EXPECTED_CONFIG.items() if k not in ('write_level_taus','write_level_min_pcts','max_replicas')}
        m=dict(format_version=1,rows=3,dimension=768,metric='ip',treatment='reference',config=EXPECTED_CONFIG,spec=dict(config=explicit),block_ids={k:'00000000-0000-0000-0000-'+str(i).zfill(12) for i,k in enumerate(['internal_node_id','leaf_node_id','posting_list_id','scalar_metadata_id','vector_data_id'],1)},integrity_audit=dict(unique_live_rows=3,duplicate_current_entries=0,unreachable_nodes=0,reachable_nodes=1,stored_nodes=1),root=dict(id=1),id_mapping=dict(path='map',format='dense-u64-le-identity',rows=3,count=3,bytes=24,global_start=0,local_start=0,sha256=hashlib.sha256(data).hexdigest()))
        return m
    def pin(self,r,m):
        p=r/'manifest.json';p.write_text(json.dumps(m));return dict(path=str(p),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest())
    def test_explicit_defaults_and_rejections(self):
        with tempfile.TemporaryDirectory() as tmp:
            r=Path(tmp);m=self.fixture(r);self.assertEqual(bridge(self.pin(r,m),3)['rows'],3)
            for edit in ['map','config','audit','root']:
                n=copy.deepcopy(m)
                if edit=='map':n['id_mapping']['path']='../map'
                if edit=='config':n['spec']['config']['split_threshold']=1
                if edit=='audit':n['integrity_audit']['duplicate_current_entries']=1
                if edit=='root':n['block_ids']['posting_list_id']=n['block_ids']['vector_data_id']
                with self.subTest(edit=edit),self.assertRaises(ValueError):bridge(self.pin(r,n),3)
            (r/'store/map').write_bytes(b'0'*24)
            with self.assertRaises(ValueError):bridge(self.pin(r,m),3)
