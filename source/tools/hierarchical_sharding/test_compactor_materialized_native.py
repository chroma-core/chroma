"""Actual materializer export and native writer; collection authority is local fake."""
import copy
import json
import os
from pathlib import Path
import struct
import subprocess
import unittest
import uuid
from .compactor_materialized import MaterializedAuthorityAdapter
from .compactor_registration import CompactorRegistration
from .dynamic_deployment import load_deployment,validate_record
from .dynamic_runtime import inventory
from .test_compactor_registration import captured_authority,CapturedAuthority
from .tree_manifest import artifact,checked_artifact,canonical_bytes,content_hash


def prepare_fixture(root,pointer):
    root=Path(root).resolve();root.mkdir(exist_ok=False)
    pointer=Path(pointer).resolve();parent=load_deployment(pointer);validate_record(parent)
    if parent['tree']['source']['row_count']!=16:raise ValueError('admitted retained16-row parent required')
    descriptor=root/'captured-parent.json';descriptor.write_bytes(canonical_bytes(parent))
    delegate=captured_authority(parent,artifact(descriptor));context=delegate.context
    delegate.source[0]['embedding'][0]=-0.0
    context_path=root/'captured-context.json';context_path.write_bytes(canonical_bytes(context))
    rows=[]
    for row in delegate.source:
        bits=list(struct.unpack('<IIII',struct.pack('<ffff',*row['embedding'])))
        rows.append(dict(log_offset=row['log_offset'],user_id=row['user_id'],embedding_bits=bits))
    c=context['parent']['collection']
    fixture=dict(collection_id=c['collection_id'],collection_version=c['version'],parent_log_position=c['log_position'],pulled_log_offset=context['compaction']['pulled_log_offset'],dimension=4,parent_deployment_sha256=content_hash(parent),ownership_generation=parent['fence'],next_global_id=16,rows=rows)
    raw=root/'materializer-input.json';raw.write_bytes(canonical_bytes(fixture))
    receipt=dict(parent_pointer=artifact(pointer),parent_deployment_sha256=content_hash(parent),context=artifact(context_path),materializer_input=artifact(raw),parent_inventory={sid:inventory(owner['checkpoint_dir']) for sid,owner in parent['owners'].items()})
    (root/'setup.json').write_bytes(canonical_bytes(receipt))
    return receipt


@unittest.skipUnless(os.environ.get('SPANN_MATERIALIZED_EVIDENCE'),'admitted actual export and native binaries required')
class NativeMaterializedRegistration(unittest.TestCase):
    def test_actual_export_to_saved_leaf_and_sealed_collection_bundle(self):
        root=Path(os.environ['SPANN_MATERIALIZED_EVIDENCE']).resolve();setup=json.loads((root/'setup.json').read_text())
        pointer=checked_artifact(setup['parent_pointer'],'.');parent=load_deployment(pointer)
        context=json.loads(checked_artifact(setup['context'],'.').read_text())
        delegate=CapturedAuthority(context,[])
        delegate.source_logs=lambda _:(_ for _ in ()).throw(AssertionError('fabricated delegate rows are forbidden'))
        export=artifact(root/'materializer-export.json');receipt=artifact(root/'materializer-export.receipt.json')
        authority=MaterializedAuthorityAdapter(delegate,export,receipt)
        session=CompactorRegistration(authority,context['parent']['collection']['collection_id'],os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],root/'private-attempt')
        actual=json.loads(Path(export['path']).read_text());request=session.task_spec()
        self.assertEqual(actual['rows'][0]['embedding_bits'][0],0x80000000)
        self.assertEqual([{k:r[k] for k in ('log_offset','global_id','user_id','embedding_bits')} for r in actual['rows']],request['rows'])
        reply=session.prepare(str(uuid.uuid4()),request)
        bundle=json.loads(checked_artifact(reply['descriptor'],'.').read_text());record=bundle['deployment'];validate_record(record)
        self.assertEqual(record['tree']['source']['row_count'],18)
        self.assertEqual(bundle['materialized_source_binding']['export'],export)
        self.assertEqual(bundle['materialized_source_binding']['receipt'],receipt)
        self.assertEqual(bundle['source_rows'],request['rows'])
        self.assertEqual(session.submit()['status'],'pending-or-unknown')
        self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(delegate.starts,1)
        delegate.state=copy.deepcopy(session.candidate_state)
        self.assertEqual(session.reconcile()['status'],'committed')
        plan=json.loads(checked_artifact(bundle['writer_execution'],'.').read_text());responses=[]
        for route in plan['routes']:
            sid=route['subtree_id'];subtree=next(s for s in record['tree']['subtrees'] if s['subtree_id']==sid)
            request=dict(op='score_selected_leaves',generation=subtree['generation'],request_id='materialized-saved-leaf',query=[0,0,0,0],nodes=[route['leaf_id']],candidate_budget=128)
            run=subprocess.run([os.environ['SPANN_DYNAMIC_BINARY'],'read','--checkpoint-dir',record['owners'][sid]['checkpoint_dir'],'--generation',str(subtree['generation'])],input=json.dumps(request)+'\n',capture_output=True,text=True,timeout=5)
            self.assertEqual(run.returncode,0,run.stderr);response=json.loads(run.stdout)
            self.assertEqual(sum(h['id']==route['id'] and h['local_id']==route['local_id'] for h in response['candidates']),1)
            responses.append(dict(route=route,request=request,response=response,stderr=run.stderr))
        checked_artifact(setup['parent_pointer'],'.')
        for sid,owner in parent['owners'].items():self.assertEqual(inventory(owner['checkpoint_dir']),setup['parent_inventory'][sid])
        self.assertFalse((root/'current.json').exists())
        (root/'saved-leaf-responses.json').write_bytes(canonical_bytes(responses))
        (root/'receipt.json').write_bytes(canonical_bytes(dict(scope='actual-materializer-export/native-saved-leaf/fake-collection-authority',materialized_export=export,export_receipt=receipt,registered_bundle=reply['descriptor'],source_rows=request['rows'],proposal=session.proposal,leaf_memberships=len(responses),no_second_pointer=True,parent_inventory_unchanged=True)))
