import importlib.util
from pathlib import Path
import tempfile, unittest, json, sys, time
HERE=Path(__file__).resolve().parent;sys.path.insert(0,str(HERE))
spec=importlib.util.spec_from_file_location('paired',HERE/'run-current-view.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Harness(unittest.TestCase):
    def test_fake_paired_exact_access_digest_and_accounting(self):
        for valid in [True,False]:
            with self.subTest(valid=valid),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);source=root/'FAKE-sparse.f32'
                with source.open('wb') as f:f.truncate(6144000000)
                vectors=dict(path=str(source),bytes=6144000000,sha256='0'*64);receipt=root/'restore.json';receipt.write_text(json.dumps(dict(status='complete',canonical=vectors,rows=2000000,dimension=768,expected_bytes=6144000000)))
                binary=root/'FAKE-native';binary.write_text('#!'+sys.executable+'''\nimport json,sys,hashlib
from pathlib import Path
p=Path(sys.argv[3]);s=json.loads(p.read_text());assert s['storage_chunk_rows']==256 and s['balance_every']==8192 and s['vector_probe_rounds']==2
current=s['current_embedding_view'];o=Path(s['output']);o.mkdir()
b=dict(vector_root_opens=32,vector_reader_cache_clears=2,vector_gets=8191,embedding_view_publications=int(current));a=dict(b);a.update(vector_gets=24575,vector_root_opens=33 if current else 98,vector_reader_cache_clears=2 if current else 6)
probe=dict(rounds=2,ids_per_round=8192,result_sha256=(__import__('hashlib').sha256(b''.join(i.to_bytes(4,'little')+bytes(3072) for _ in range(2) for i in range(8192))).hexdigest() if not current or VALID else 'wrong'),seconds=.1,counters_before={'0':b},counters_after={'0':a},reads_before={'0':dict(valid=True,open_reader_roots=[],readers=[])},reads_after={'0':dict(valid=True,open_reader_roots=[],readers=[])},storage_before=dict(overflowed=False,objects={}),storage_after=dict(overflowed=False,objects={}))
v=dict(status='passed',spec=s,spec_sha256=hashlib.sha256(p.read_bytes()).hexdigest(),census=dict(unique_live_rows=8192,duplicate_current_entries=0,unreachable_nodes=0),genesis_vector_view=dict(embedding_root='genesis'),cuts=[dict(chunk_roots=[dict(start=1+i*256,end=min(8192,1+(i+1)*256),root=str(i)) for i in range(32)],prebalance_phase=dict(nodes=1,levels={'0':1},leaves=[dict(live_rows=8192)]),probe=probe,publication_seconds=.001,vector_reads_after_balance=None)])
(o/'result.json').write_text(json.dumps(v));print(json.dumps(v))
'''.replace('VALID',repr(valid)));binary.chmod(0o755)
                value=m.run(dict(binary=m.descriptor(binary),restore_receipt=m.descriptor(receipt),vectors=vectors,output=str(root/'output'),rows=8192,dimension=768,balance_every=8192,threads=2,arm_seconds=30,total_seconds=60,deadline_unix=time.time()+60,config=dict(FAKE=True)))
                self.assertEqual(value['status'],'passed' if valid else 'failed',value)
    def test_expanded_budget_rejected(self):
        with self.assertRaises(ValueError):m.run(dict(deadline_unix=time.time()+90,rows=8192,dimension=768,balance_every=8192,threads=2,arm_seconds=30,total_seconds=90))
if __name__=='__main__':unittest.main(verbosity=2)
