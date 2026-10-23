"""Real topology replacements keep ownership pages in the saved deployment CAS."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from . import ownership_pages as pages
from .test_dynamic_write import make_parent,evidence_case
from .dynamic_topology import (split_request,merge_request,reassign_request,
                               prepare_topology_record,publish_topology_record)
from .dynamic_deployment import (make_record,publish_deployment,load_deployment,content_hash)
from .dynamic_runtime import DeploymentReader


@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native writer required')
class PagedTopology(unittest.TestCase):
    def test_real_split_merge_reassign_and_placement_preserve_complete_ownership(self):
        with evidence_case('paged-topology-success') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,vectors,probe=make_parent(root,binary,page_rows=4)
            pinned=DeploymentReader(pointer,binary);original=pages.audit(parent['ownership_pages']);receipts=[]
            try:
                requests=[lambda p:split_request(p,'s0',[0,1,2,3],'left','right'),lambda p:merge_request(p,['left','right'],'s0'),lambda p:reassign_request(p,'s0','s1',[0,1])]
                for step,request in enumerate(requests):
                    previous=parent;output=root/('topology-'+str(step))
                    publish_topology_record(pointer,parent,binary,request(parent),output,probe)
                    parent=load_deployment(pointer)
                    self.assertEqual(parent['ownership_pages']['parent_sha256'],content_hash(previous['ownership_pages']))
                    self.assertEqual(pages.audit(parent['ownership_pages']).root()['global_count'],16)
                    current=DeploymentReader(pointer,binary)
                    try:
                        result=current.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
                        self.assertEqual(result['ids'],list(range(16)))
                        self.assertEqual(result['distances'],[sum(x*x for x in row) for row in vectors])
                        receipts.append(dict(deployment_sha256=content_hash(parent),page_root_sha256=content_hash(parent['ownership_pages']),query=result))
                    finally:current.close()
                moved=make_record(parent['tree'],parent['owners'],parent)
                publish_deployment(pointer,moved,content_hash(parent),probe)
                self.assertEqual(load_deployment(pointer)['ownership_pages'],parent['ownership_pages'])
                self.assertEqual(pages.lookup(original,'s0',0),0)
                self.assertEqual(pinned.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids'],list(range(16)))
                (root/'topology-receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
            finally:pinned.close()

    def test_partial_replacement_page_write_retains_real_builds_without_publication(self):
        with evidence_case('paged-topology-failure') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,_,probe=make_parent(root,binary,page_rows=4);head=pointer.read_bytes()
            request=split_request(parent,'s0',[0,1,2,3],'left','right');output=root/'failed';original=pages._page;calls=[]
            def fail(*args):
                calls.append(1)
                if len(calls)==2:raise OSError('replacement page flush failure')
                return original(*args)
            with patch.object(pages,'_page',side_effect=fail):
                with self.assertRaisesRegex(OSError,'page flush failure'):publish_topology_record(pointer,parent,binary,request,output,probe)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertTrue((output/'index-left/manifest-2.json').exists());self.assertTrue((output/'index-right/manifest-2.json').exists())
            self.assertEqual(json.loads(Path(str(output)+'.transaction.json').read_text())['status'],'failed_unpublished')
            publish_topology_record(pointer,parent,binary,request,root/'fresh',probe)
            self.assertEqual(load_deployment(pointer)['ownership_pages']['global_count'],16)

    def test_stale_topology_root_and_changed_placement_root_rejected(self):
        with evidence_case('paged-topology-stale') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,_,probe=make_parent(root,binary,page_rows=4)
            candidate=prepare_topology_record(binary,parent,split_request(parent,'s0',[0,1,2,3],'left','right'),root/'private')
            alternate=pages.replace(pages.audit(parent['ownership_pages']),{'s0':list(range(8)),'s1':list(range(8,16))},root/'alternate')
            bad=make_record(parent['tree'],parent['owners'],parent,ownership_pages=alternate)
            with self.assertRaisesRegex(ValueError,'preserve exact ownership'):publish_deployment(pointer,bad,content_hash(parent),probe)
            winner=make_record(parent['tree'],parent['owners'],parent);publish_deployment(pointer,winner,content_hash(parent),probe);head=pointer.read_bytes()
            with self.assertRaisesRegex(ValueError,'compare-and-swap'):publish_deployment(pointer,candidate,content_hash(parent),probe)
            self.assertEqual(pointer.read_bytes(),head)


if __name__=='__main__':unittest.main()
