"""Real topology replacement publishes saved roots and both ownership formats."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from . import ownership_trie as trie
from .test_dynamic_write import make_parent,evidence_case
from .dynamic_topology import (split_request,merge_request,reassign_request,prepare_topology_record,publish_topology_record)
from .dynamic_deployment import make_record,publish_deployment,load_deployment,content_hash
from .dynamic_runtime import DeploymentReader


@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native writer required')
class TrieTopology(unittest.TestCase):
    def test_split_merge_reassign_scalar_coverage_oldpins_and_placement(self):
        with evidence_case('trie-topology-success') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,vectors,probe=make_parent(root,binary,page_rows=4,trie_fanout=4)
            old=trie.PinnedReader(trie.audit(parent['ownership_trie'])[0]);oldquery=DeploymentReader(pointer,binary);receipts=[]
            try:
                requests=[lambda p:split_request(p,'s0',[0,1,2,3],'left','right'),lambda p:merge_request(p,['left','right'],'s0'),lambda p:reassign_request(p,'s0','s1',[0,1])]
                for index,request in enumerate(requests):
                    previous=parent;publish_topology_record(pointer,parent,binary,request(parent),root/str(index),probe);parent=load_deployment(pointer)
                    self.assertEqual(parent['ownership_trie']['parent_sha256'],content_hash(previous['ownership_trie']))
                    receipt,_,maps=trie.audit_with_maps(parent['ownership_trie']);self.assertEqual(sorted(v for rows in maps.values() for v in rows),list(range(16)))
                    current=DeploymentReader(pointer,binary)
                    try:
                        result=current.query([0,0,0,0],16,beam_width=None,candidate_budget=16)
                        self.assertEqual(result['ids'],list(range(16)));self.assertEqual(result['distances'],[sum(v*v for v in row) for row in vectors])
                        receipts.append(dict(deployment_sha256=content_hash(parent),trie_sha256=receipt.sha256,maps=maps,query=result))
                    finally:current.close()
                moved=make_record(parent['tree'],parent['owners'],parent);publish_deployment(pointer,moved,content_hash(parent),probe)
                self.assertEqual(load_deployment(pointer)['ownership_trie'],parent['ownership_trie'])
                self.assertEqual(old.lookup('s0',0),0);self.assertEqual(oldquery.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids'],list(range(16)))
                (root/'receipts.json').write_text(json.dumps(receipts,indent=2)+'\n')
            finally:oldquery.close()

    def test_partial_replacement_descriptor_failure_and_fresh_parent_retry(self):
        with evidence_case('trie-topology-failure') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,_,probe=make_parent(root,binary,page_rows=4,trie_fanout=4);head=pointer.read_bytes();output=root/'failed'
            request=split_request(parent,'s0',[0,1,2,3],'left','right');original=trie.Store.put;calls=[]
            def fail(store,*args):
                calls.append(1)
                if len(calls)==2:raise OSError('replacement descriptor flush failure')
                return original(store,*args)
            with patch.object(trie.Store,'put',new=fail):
                with self.assertRaisesRegex(OSError,'descriptor flush failure'):publish_topology_record(pointer,parent,binary,request,output,probe)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertTrue((output/'index-left/manifest-2.json').exists());self.assertTrue((output/'index-right/manifest-2.json').exists())
            self.assertTrue(list((output/'ownership-pages').iterdir()));self.assertTrue(list((output/'ownership-trie').iterdir()))
            self.assertEqual(json.loads(Path(str(output)+'.transaction.json').read_text())['status'],'failed_unpublished')
            publish_topology_record(pointer,parent,binary,request,root/'fresh',probe)
            self.assertEqual(load_deployment(pointer)['ownership_trie']['global_count'],16)

    def test_stale_replacement_and_live_global_set_mismatch_rejected(self):
        with evidence_case('trie-topology-stale') as root:
            binary=os.environ['SPANN_DYNAMIC_BINARY'];parent,pointer,_,probe=make_parent(root,binary,page_rows=4,trie_fanout=4)
            candidate=prepare_topology_record(binary,parent,split_request(parent,'s0',[0,1,2,3],'left','right'),root/'private')
            wrong=dict(candidate['ownership_trie'],parent_sha256='0'*64)
            with self.assertRaises(ValueError):trie.verify_replacement(trie.audit(parent['ownership_trie'])[0],wrong)
            winner=make_record(parent['tree'],parent['owners'],parent);publish_deployment(pointer,winner,content_hash(parent),probe);head=pointer.read_bytes()
            with self.assertRaisesRegex(ValueError,'compare-and-swap'):publish_deployment(pointer,candidate,content_hash(parent),probe)
            self.assertEqual(pointer.read_bytes(),head)


if __name__=='__main__':unittest.main()
