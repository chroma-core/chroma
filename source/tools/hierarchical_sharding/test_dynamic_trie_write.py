"""Real saved roots, flat-page oracle and descriptor trie share one CAS."""
import json
import os
from pathlib import Path
import struct
import unittest
from unittest.mock import patch
from . import dynamic_write,ownership_trie as trie
from .test_dynamic_write import make_parent,evidence_case
from .dynamic_deployment import (make_record,publish_deployment,load_deployment,content_hash)
from .dynamic_runtime import DeploymentReader


@unittest.skipUnless(os.environ.get('SPANN_WRITE_PLANNER_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native compactor/reader required')
class TriePublication(unittest.TestCase):
    def prepare(self,root):
        reader=os.environ['SPANN_DYNAMIC_BINARY'];planner=os.environ['SPANN_WRITE_PLANNER_BINARY']
        parent,pointer,_,probe=make_parent(root,reader,page_rows=4,trie_fanout=4)
        corpus=root/'append.f32';corpus.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+struct.pack('<ffff',.5,1,0,0)+struct.pack('<ffff',17,1,0,0))
        return reader,planner,parent,pointer,probe,corpus

    def test_complete_roots_repeated_append_old_pins_and_placement(self):
        with evidence_case('trie-publication-success') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root)
            old=trie.PinnedReader(trie.audit(parent['ownership_trie'])[0]);query=DeploymentReader(pointer,reader)
            try:
                dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,root/'first',probe,max_rows=128)
                first=load_deployment(pointer)
                self.assertEqual(first['tree']['source']['row_count'],18);self.assertEqual(first['ownership_pages']['global_count'],18);self.assertEqual(first['ownership_trie']['global_count'],18)
                pinned=trie.PinnedReader(trie.audit(first['ownership_trie'])[0])
                self.assertEqual(pinned.lookup('s0',8),16);self.assertEqual(pinned.lookup('s1',8),17)
                again=root/'append-again.f32';again.write_bytes(corpus.read_bytes()+struct.pack('<ffff',18,1,0,0))
                dynamic_write.publish_global_append(pointer,first,planner,reader,again,root/'second',probe,max_rows=128)
                second=load_deployment(pointer)
                self.assertEqual(second['ownership_trie']['global_count'],19);self.assertEqual(second['ownership_trie']['parent_sha256'],content_hash(first['ownership_trie']))
                self.assertEqual(pinned.lookup('s1',8),17);self.assertEqual(old.lookup('s0',7),7)
                self.assertEqual(query.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids'],list(range(16)))
                current=DeploymentReader(pointer,reader)
                try:self.assertEqual(set(current.query([0,0,0,0],19,beam_width=None,candidate_budget=19)['ids']),set(range(19)))
                finally:current.close()
                placement=make_record(second['tree'],second['owners'],second);publish_deployment(pointer,placement,content_hash(second),probe)
                self.assertEqual(load_deployment(pointer)['ownership_trie'],second['ownership_trie'])
                (root/'root-receipts.json').write_text(json.dumps(dict(parent=content_hash(parent),first=content_hash(first),second=content_hash(second),first_proof=json.loads((root/'first/ownership-trie-verification.json').read_text()),second_proof=json.loads((root/'second/ownership-trie-verification.json').read_text()),old_lookup_metrics=old.store.metrics,pinned_root_owned_bytes=old.root_owned_bytes),indent=2)+'\n')
            finally:query.close()

    def test_partial_descriptor_write_after_saved_indexes_and_flatpages_blocks_cas(self):
        with evidence_case('trie-publication-partial') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root);head=pointer.read_bytes();output=root/'failed';original=trie.Store.put;calls=[]
            def fail(store,*args):
                calls.append(1)
                if len(calls)==2:raise OSError('descriptor flush failure')
                return original(store,*args)
            with patch.object(trie.Store,'put',new=fail):
                with self.assertRaisesRegex(OSError,'descriptor flush failure'):dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,output,probe,max_rows=128)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertTrue((output/'index-s0/manifest-2.json').exists());self.assertTrue((output/'index-s1/manifest-2.json').exists())
            self.assertTrue(list((output/'ownership-pages').iterdir()));self.assertTrue(list((output/'ownership-trie').iterdir()))
            self.assertEqual(json.loads(Path(str(output)+'.transaction.json').read_text())['status'],'failed_unpublished')
            dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,root/'fresh',probe,max_rows=128)
            self.assertEqual(load_deployment(pointer)['ownership_trie']['global_count'],18)

    def test_stale_complete_root_cas_keeps_saved_trie_and_flatpages_together(self):
        with evidence_case('trie-publication-stale') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root)
            candidate=dynamic_write.prepare_global_append(planner,reader,parent,corpus,root/'private',max_rows=128)
            winner=make_record(parent['tree'],parent['owners'],parent);publish_deployment(pointer,winner,content_hash(parent),probe);head=pointer.read_bytes()
            with self.assertRaisesRegex(ValueError,'compare-and-swap'):publish_deployment(pointer,candidate,content_hash(parent),probe)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertEqual(load_deployment(pointer)['ownership_trie'],parent['ownership_trie'])


if __name__=='__main__':unittest.main()
