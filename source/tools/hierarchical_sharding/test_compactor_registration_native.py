"""One admitted local producer fixture; authority remains an in-process fake."""
import copy
import json
import os
from pathlib import Path
import subprocess
import unittest
import uuid
from .compactor_registration import CompactorRegistration,HIERARCHY_BUNDLE
from .dynamic_deployment import load_deployment,validate_record
from .test_compactor_registration import captured_authority
from .tree_manifest import artifact,canonical_bytes

@unittest.skipUnless(os.environ.get('SPANN_COMPACTOR_EVIDENCE'),'reviewed native fixture requires explicit evidence root')
class NativeRegistration(unittest.TestCase):
    def test_saved_selected_leaf_bundle_without_second_pointer(self):
        root=Path(os.environ['SPANN_COMPACTOR_EVIDENCE']).resolve();root.mkdir(exist_ok=False)
        pointer=Path(os.environ['SPANN_COMPACTOR_PARENT']);before=pointer.read_bytes()
        parent=load_deployment(pointer);self.assertEqual(parent['tree']['source']['row_count'],16)
        reference=root/'captured-parent.json';reference.write_bytes(canonical_bytes(parent))
        authority=captured_authority(parent,artifact(reference))
        session=CompactorRegistration(authority,authority.state['collection']['collection_id'],os.environ['SPANN_WRITE_PLANNER_BINARY'],os.environ['SPANN_DYNAMIC_BINARY'],root/'private-attempt')
        request=session.task_spec();reply=session.prepare(str(uuid.uuid4()),request)
        bundle=json.loads(Path(reply['descriptor']['path']).read_text());record=bundle['deployment'];validate_record(record)
        self.assertEqual(record['tree']['source']['row_count'],18)
        self.assertEqual(pointer.read_bytes(),before)
        self.assertFalse((root/'current.json').exists())
        self.assertEqual(session.submit()['status'],'pending-or-unknown')
        self.assertEqual(session.submit()['status'],'pending-or-unknown');self.assertEqual(authority.starts,1)
        authority.state=copy.deepcopy(session.candidate_state)
        self.assertEqual(session.reconcile()['status'],'committed')
        selected=next(row for row in session.proposal['segment_flush_info'] if row['segment_id']==session.vector_id)
        self.assertEqual(json.loads(selected['file_paths'][HIERARCHY_BUNDLE][0]),reply['descriptor'])
        self.assertEqual(selected['file_paths']['unrelated'],['preserve'])
        plan=json.loads(Path(bundle['writer_execution']['path']).read_text());responses=[]
        for route in plan['routes']:
            sid=route['subtree_id'];subtree=next(s for s in record['tree']['subtrees'] if s['subtree_id']==sid)
            request=dict(op='score_selected_leaves',generation=subtree['generation'],request_id='registered-leaf',query=[0,0,0,0],nodes=[route['leaf_id']],candidate_budget=128)
            run=subprocess.run([os.environ['SPANN_DYNAMIC_BINARY'],'read','--checkpoint-dir',record['owners'][sid]['checkpoint_dir'],'--generation',str(subtree['generation'])],input=json.dumps(request)+'\n',capture_output=True,text=True,timeout=5)
            self.assertEqual(run.returncode,0,run.stderr);response=json.loads(run.stdout)
            self.assertEqual(sum(hit['id']==route['id'] and hit['local_id']==route['local_id'] for hit in response['candidates']),1)
            responses.append(dict(route=route,request=request,response=response,stderr=run.stderr))
        (root/'registered-leaf-responses.json').write_bytes(canonical_bytes(responses))
        (root/'receipt.json').write_bytes(canonical_bytes(dict(scope='local-native-producer-and-fake-registration-authority',parent_pointer_unchanged=True,no_second_pointer=True,reply=reply,proposal=session.proposal,selected_leaf_count=len(responses))))
