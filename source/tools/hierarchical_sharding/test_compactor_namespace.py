"""Captured-view and preparation hook contracts; no provider or SysDb proof."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from .compactor_namespace import current_view, NamespacePreparation
from .compactor_registration import HIERARCHY_BUNDLE
from .test_compactor_registration import RegistrationOutcomes
from .tree_manifest import artifact,canonical_bytes

class CurrentView(unittest.TestCase):
    def test_construction_snapshot_is_separate_but_configuration_is_bound(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            def write(name,value):
                path=root/name;path.write_bytes(canonical_bytes(value));return artifact(path)
            collection={'collection_id':'collection','version':3,'log_position':10,'total_records_post_compaction':4,'schema':None}
            wire={'id':'collection','version':2,'log_position':8,'dimension':2,'schema':None,'configuration_json':{'metric':'l2'}}
            binding=dict(wire=wire,total_records_post_compaction=4,size_bytes_post_compaction=32,last_compaction_time_secs=1,version_file_path=None,lineage_file_path=None,updated_at={'secs_since_epoch':1,'nanos_since_epoch':0},compaction_failure_count=0,database_id='database',root_collection_id=None)
            namespace=dict(record_segment_id='00000000-0000-0000-0000-000000000002',record_files={'record':['saved']},collection=binding,row_count=4,canonical_vectors={'canonical':'same'},owner_maps=[{'map':'same'}],construction_parent_deployment_sha256='a'*64,construction_parent_ownership_generation=1,parent_namespace_sha256=None)
            ns=write('namespace.json',namespace)
            receipt=write('construction.json',dict(namespace_root=ns,construction_parent_deployment_sha256='a'*64,construction_parent_ownership_generation=1,parent_namespace_sha256=None))
            metadata=dict(record_segment_id=namespace['record_segment_id'],namespace_root=ns,construction_receipt=receipt)
            deployment=dict(fence=9,canonical_vectors=namespace['canonical_vectors'],tree={'dimension':2,'source':{'row_count':4},'subtrees':[{'subtree_id':'s0','local_to_global':namespace['owner_maps'][0]}]})
            bundle=write('bundle.json',dict(deployment=deployment,hierarchy_metadata=metadata,collection_parent={'collection':{'collection_id':'collection','version':2}},compaction={'pulled_log_offset':10}))
            current=copy.deepcopy(binding);current['wire'].update(version=3,log_position=10);current['updated_at']['secs_since_epoch']=2
            context=dict(record_collection_binding=current,vector_segment_id='00000000-0000-0000-0000-000000000001',parent=dict(collection=collection,segments=[dict(segment_id='00000000-0000-0000-0000-000000000001',scope='VECTOR',file_paths={HIERARCHY_BUNDLE:[canonical_bytes(bundle).decode()]}),dict(segment_id=namespace['record_segment_id'],scope='RECORD',type='urn:chroma:segment/record/blockfile',file_paths=namespace['record_files'])]))
            with patch('tools.hierarchical_sharding.compactor_namespace.validate_record'):
                view=current_view(context)
                self.assertEqual(view['current_parent']['ownership_generation'],9)
                self.assertNotEqual(view['current_parent']['deployment_sha256'],'a'*64)
                changed=copy.deepcopy(context);changed['record_collection_binding']['wire']['configuration_json']['metric']='ip'
                with self.assertRaisesRegex(ValueError,'configuration'):current_view(changed)
                changed=copy.deepcopy(context);changed['parent']['segments'][1]['file_paths']['record']=['wrong']
                with self.assertRaisesRegex(ValueError,'record roots'):current_view(changed)

class HookPoison(unittest.TestCase):
    def test_failed_record_producer_cannot_repeat(self):
        class Producer:
            calls=0
            def prepare(self,*args):self.calls+=1;raise OSError('record flush failed')
        producer=Producer();adapter=NamespacePreparation(object(),producer);adapter.context={};adapter.view={}
        with patch('tools.hierarchical_sharding.compactor_namespace.current_view',return_value={}):
            with self.assertRaises(OSError):adapter.prepare_hierarchy_metadata({}, {})
            with self.assertRaisesRegex(ValueError,'cannot repeat'):adapter.prepare_hierarchy_metadata({}, {})
        self.assertEqual(producer.calls,1)

class SubmitValidation(RegistrationOutcomes):
    def test_namespace_changed_after_prepare_blocks_registration_and_poison_replay(self):
        session=self.session;session.reply={'prepared':True}
        (session.output/'bundle.json').write_bytes(canonical_bytes({'deployment':{},'hierarchy_metadata':{'root':'changed'},'segment_outputs':{}}))
        def reject(*args):raise ValueError('candidate record audit changed')
        session.authority.validate_hierarchy_metadata=reject
        with self.assertRaisesRegex(ValueError,'candidate record audit'):self.submit()
        self.assertEqual(session.authority.starts,0)
        self.assertIsNone(session.reply)
        self.assertEqual(session.phase,'poisoned-private-attempt')
