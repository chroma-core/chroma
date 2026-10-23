"""Boundary unit tests; manually supplied exports here are not runtime proof."""
import copy
import json
from pathlib import Path
import struct
import tempfile
import unittest
from unittest.mock import patch
from .compactor_materialized import MaterializedAuthorityAdapter,PRODUCERS
from .test_compactor_registration import captured_authority
from .tree_manifest import artifact,canonical_bytes,content_hash,file_hash

class MaterializedBoundary(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name)
        store=self.root/'store';store.mkdir();canonical=self.root/'vectors';canonical.write_bytes(bytes(16));model=self.root/'model';model.write_text('{}')
        self.parent={'fence':1,'tree':{'metric':'l2','dimension':4,'source':{'row_count':1},'model':artifact(model)},'owners':{'s0':{'checkpoint_dir':str(store)}},'canonical_vectors':artifact(canonical)}
        path=self.root/'parent';path.write_bytes(canonical_bytes(self.parent));self.delegate=captured_authority(self.parent,artifact(path));c=self.delegate.context['parent']['collection']
        bits=lambda vector:list(struct.unpack('<IIII',struct.pack('<ffff',*vector)))
        self.value=dict(collection_id=c['collection_id'],collection_version=c['version'],parent_log_position=10,pulled_log_offset=12,parent_deployment_sha256=content_hash(self.parent),ownership_generation=1,rows=[dict(log_offset=11+i,global_id=1+i,record_offset_id=1+i,user_id='new-'+str(i),embedding_bits=bits(v)) for i,v in enumerate([[-0.,1.,0.,0.],[17.,1.,0.,0.]])])
    def wrapper(self,value=None):
        export=self.root/'export';export.write_bytes(canonical_bytes(self.value if value is None else value))
        fixture=self.root/'fixture';fixture.write_text('unit-only');binary=self.root/'binary';binary.write_text('unit-only')
        repo=Path(__file__).resolve().parents[2]
        proof=dict(format_version=1,export=artifact(export),fixture_input=artifact(fixture),runtime_binary=artifact(binary),producer_sources={n:file_hash(repo/n) for n in PRODUCERS})
        receipt=self.root/'receipt';receipt.write_bytes(canonical_bytes(proof))
        wrapper=MaterializedAuthorityAdapter(self.delegate,artifact(export),artifact(receipt));context=wrapper.capture(self.delegate.context['parent']['collection']['collection_id'])
        return wrapper,context
    def test_exact_context_actual_ids_and_negative_zero_roundtrip(self):
        wrapper,context=self.wrapper()
        self.delegate.source_logs=lambda _:(_ for _ in ()).throw(AssertionError('manual delegate rows must not be used'))
        with patch('tools.hierarchical_sharding.compactor_materialized.validate_record'):
            rows=wrapper.source_logs(context);binding=wrapper.source_binding(context)
        self.assertEqual([r['global_id'] for r in rows],[1,2])
        self.assertEqual(struct.unpack('<I',struct.pack('<f',rows[0]['embedding'][0]))[0],0x80000000)
        self.assertEqual(binding['parent_deployment_sha256'],content_hash(self.parent))
        changed=copy.deepcopy(context);changed['compaction']['pulled_log_offset']=13
        with self.assertRaises(ValueError):wrapper.source_logs(changed)
    def test_malformed_rows_fences_bits_and_extra_fields_reject(self):
        mutations=[lambda v:v.update(collection_version=True),lambda v:v.update(ownership_generation=2),lambda v:v.update(parent_deployment_sha256='b'*64),lambda v:v['rows'][0].update(record_offset_id=True),lambda v:v['rows'][0].update(global_id=99),lambda v:v['rows'][0]['embedding_bits'].__setitem__(0,True),lambda v:v['rows'][0]['embedding_bits'].__setitem__(0,2**32),lambda v:v['rows'][0]['embedding_bits'].__setitem__(0,0x7fc00000),lambda v:v['rows'][1].update(user_id='new-0'),lambda v:v['rows'][0].update(extra='ignored'),lambda v:v['rows'].reverse()]
        for mutate in mutations:
            value=copy.deepcopy(self.value);mutate(value);wrapper,context=self.wrapper(value)
            with patch('tools.hierarchical_sharding.compactor_materialized.validate_record'):
                with self.assertRaises(ValueError):wrapper.source_logs(context)
    def test_source_artifact_change_and_wrong_capture_reject(self):
        wrapper,context=self.wrapper()
        with patch('tools.hierarchical_sharding.compactor_materialized.validate_record'):wrapper.source_logs(context)
        path=Path(wrapper.export['path']);path.write_bytes(path.read_bytes().replace(b'new-0',b'new-x'))
        with self.assertRaises(ValueError):wrapper.source_binding(context)
        self.delegate.context['compaction']['session_id']='00000000-0000-0000-0000-000000000000'
        with self.assertRaises(ValueError):wrapper.capture(context['parent']['collection']['collection_id'])
