"""In-process authority contract tests; these do not exercise SysDb RPCs."""
import copy
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from .compactor_registration import CompactorRegistration
from .tree_manifest import canonical_bytes, content_hash

class Authority:
    def __init__(self, parent):
        self.state=copy.deepcopy(parent); self.starts=0; self.offset=10
        self.terminal=False; self.read_error=False; self.log_error=False
    def observe(self, collection):
        if self.read_error: raise OSError('authority unavailable')
        return copy.deepcopy(self.state)
    def start_registration(self, attempt, proposal):
        self.starts+=1; self.proposal=copy.deepcopy(proposal)
        raise TimeoutError('accepted but acknowledgement unavailable')
    def attempt_status(self, attempt, digest):
        return 'authoritative-terminal-rejection' if self.terminal else 'pending'
    def log_position(self, collection): return self.offset
    def reconcile_log_if_current(self, expected, offset):
        if self.state!=expected: raise ValueError('candidate no longer current')
        if self.log_error: raise OSError('log unavailable')
        self.offset=offset

class RegistrationOutcomes(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.parent={'collection':{'collection_id':'collection','version':3},'segments':[]}
        self.session=CompactorRegistration.__new__(CompactorRegistration)
        s=self.session; s.output=Path(self.temp.name); s.context={};s.events=[]
        s.parent_state=copy.deepcopy(self.parent)
        s.candidate_state=copy.deepcopy(self.parent); s.candidate_state['collection']['version']=4
        s.proposal={'collection_id':'collection','log_position':11}
        s.proposal_sha256=content_hash(s.proposal)
        s.bundle_reference={'path':str(s.output/'bundle.json')}
        (s.output/'bundle.json').write_bytes(canonical_bytes({'deployment':{}}))
        s.authority=Authority(self.parent); s.attempt=None;s.phase='prepared'
        s._health=lambda:None
    def submit(self):
        with patch('tools.hierarchical_sharding.compactor_registration.checked_artifact'),patch('tools.hierarchical_sharding.compactor_registration.validate_record'):
            return self.session.submit()
    def reconcile(self):
        with patch('tools.hierarchical_sharding.compactor_registration.checked_artifact'):
            return self.session.reconcile()
    def test_pending_parent_is_unknown_and_never_resubmits(self):
        self.assertEqual(self.submit()['status'],'pending-or-unknown')
        self.assertEqual(self.submit()['status'],'pending-or-unknown')
        self.assertEqual(self.session.authority.starts,1)
    def test_late_visibility_and_log_failure_do_not_repeat_registration(self):
        self.submit(); s=self.session; a=s.authority
        a.state=copy.deepcopy(s.candidate_state)
        self.assertEqual(self.reconcile(),{'status':'committed','log_synchronized':False})
        a.log_error=True
        with patch('tools.hierarchical_sharding.compactor_registration.checked_artifact'):
            with self.assertRaises(OSError): s.reconcile_log()
            a.log_error=False
            self.assertTrue(s.reconcile_log()['log_synchronized'])
        self.assertEqual(a.starts,1)
    def test_only_authoritative_terminal_rejection_classifies_parent(self):
        self.submit(); self.session.authority.terminal=True
        self.assertEqual(self.reconcile()['status'],'terminal-rejected')
    def test_descendant_wrong_bundle_and_read_failure_remain_unknown(self):
        self.submit(); a=self.session.authority
        for state in ({'collection':{'collection_id':'collection','version':5},'segments':[]},dict(self.session.candidate_state,segments=[{'unexpected':True}])):
            a.state=state
            self.assertEqual(self.reconcile()['status'],'pending-or-unknown')
        a.read_error=True
        self.assertEqual(self.reconcile()['status'],'pending-or-unknown')
    def test_descendant_blocks_stale_log_repair(self):
        self.submit(); a=self.session.authority
        a.state['collection']['version']=5
        with self.assertRaises(ValueError): self.session.reconcile_log()
        self.assertEqual(a.offset,10)
    def test_changed_prepared_proposal_rejects_before_registration(self):
        self.session.proposal['log_position']=12
        with self.assertRaises(ValueError): self.submit()
        self.assertEqual(self.session.authority.starts,0)
    def test_conflicting_task_replay_never_mutates(self):
        s=self.session;s.task_id='00000000-0000-0000-0000-000000000001'
        s.task_bytes=canonical_bytes({'embedding_bits':[1]});s.reply={'ok':True}
        with self.assertRaises(ValueError):s.prepare(s.task_id,{'embedding_bits':[2]})
        self.assertEqual(s.prepare(s.task_id,{'embedding_bits':[1]}),{'ok':True})
    def test_poisoned_replay_requires_fresh_parent_session(self):
        s=self.session;s.task_id='00000000-0000-0000-0000-000000000001'
        s.task_bytes=canonical_bytes({});s.reply=None
        with self.assertRaisesRegex(ValueError,'fresh immutable-parent'):s.prepare(s.task_id,{})

if __name__=='__main__':unittest.main()

class CapturedAuthority(Authority):
    def __init__(self,context,source):
        super().__init__(context['parent']);self.context=context;self.source=source
    def capture(self,collection):return copy.deepcopy(self.context)
    def source_logs(self,context):return copy.deepcopy(self.source)
    def current_session(self,collection):return {k:self.context['compaction'][k] for k in ('job_id','session_id')}
    def segment_outputs(self,context):return copy.deepcopy(context['parent']['segments'])


def captured_authority(parent,reference):
    import uuid
    from .compactor_registration import HIERARCHY_BUNDLE
    uid=lambda:str(uuid.uuid4())
    collection=uid();vector=uid();other=uid()
    state={'collection':{'collection_id':collection,'tenant_id':'tenant','database_name':'database','version':3,'log_position':10,'total_records_post_compaction':parent['tree']['source']['row_count'],'size_bytes_post_compaction':256,'schema':None},'segments':[{'segment_id':vector,'scope':'VECTOR','type':'urn:chroma:segment/vector/spann','file_paths':{HIERARCHY_BUNDLE:[canonical_bytes(reference).decode().strip()],'unrelated':['preserve']}},{'segment_id':other,'scope':'RECORD','type':'record','file_paths':{'record':['parent']}}]}
    context={'parent':state,'vector_segment_id':vector,'compaction':{'job_id':uid(),'session_id':uid(),'pulled_log_offset':12,'target_total_records':parent['tree']['source']['row_count']+2,'target_logical_size_bytes':288}}
    source=[dict(operation='add',materialized_operation='add_new',log_offset=11+i,global_id=parent['tree']['source']['row_count']+i,user_id='new-'+str(i),embedding=v) for i,v in enumerate([[.5,1.,0.,0.],[17.,1.,0.,0.]])]
    return CapturedAuthority(context,source)

class CapturedPreparation(unittest.TestCase):
    def test_source_capture_bundle_and_poisoned_partial_preparation(self):
        import json
        import uuid
        from .tree_manifest import artifact
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);store=root/'parent-store';store.mkdir()
            canonical=root/'parent.f32';canonical.write_bytes(bytes(16))
            model=root/'model.json';model.write_text('{}')
            parent={'fence':1,'tree':{'metric':'l2','dimension':4,'source':{'row_count':1},'model':artifact(model)},'owners':{'s0':{'checkpoint_dir':str(store)}},'canonical_vectors':artifact(canonical)}
            descriptor=root/'parent.json';descriptor.write_bytes(canonical_bytes(parent))
            authority=captured_authority(parent,artifact(descriptor))
            with patch('tools.hierarchical_sharding.compactor_registration.validate_record'):
                session=CompactorRegistration(authority,authority.state['collection']['collection_id'],'planner','reader',root/'attempt')
                def prepare(planner,reader,parent,corpus,output,timeout):
                    output.mkdir();(output/'orphan').write_text('retained partial')
                    raise OSError('selected writer flush failed')
                task=str(uuid.uuid4());spec=session.task_spec()
                # Source input is an owned capture, not a mutable collector dictionary.
                authority.source[0]['embedding'][0]=999
                self.assertNotEqual(session.task_spec()['rows'][0]['embedding_bits'],[999])
                with patch('tools.hierarchical_sharding.compactor_registration.prepare_global_append',side_effect=prepare) as producer:
                    with self.assertRaises(OSError):session.prepare(task,spec)
                    with self.assertRaisesRegex(ValueError,'fresh immutable-parent'):session.prepare(task,spec)
                    self.assertEqual(producer.call_count,1)
                journal=json.loads((root/'attempt/session.json').read_text())
                self.assertEqual(journal['events'][-1]['phase'],'poisoned-private-attempt')
                self.assertTrue((root/'attempt/private-writers/orphan').exists())
                self.assertEqual(authority.starts,0)
