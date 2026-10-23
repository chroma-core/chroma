"""Real saved-index and ownership-page roots share one deployment publication."""
import copy
import json
import os
from pathlib import Path
import struct
import unittest
from unittest.mock import patch
from . import dynamic_write,ownership_pages
from .test_dynamic_write import make_parent,evidence_case
from .dynamic_deployment import (load_deployment,make_record,publish_deployment,content_hash,validate_record)
from .dynamic_runtime import DeploymentReader


@unittest.skipUnless(os.environ.get('SPANN_WRITE_PLANNER_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted compactor and reader required')
class PagedPublication(unittest.TestCase):
    def prepare(self,root):
        reader=os.environ['SPANN_DYNAMIC_BINARY'];planner=os.environ['SPANN_WRITE_PLANNER_BINARY']
        parent,pointer,vectors,probe=make_parent(root,reader,page_rows=4)
        corpus=root/'append.f32';corpus.write_bytes(Path(parent['canonical_vectors']['path']).read_bytes()+struct.pack('<ffff',.5,1,0,0)+struct.pack('<ffff',17,1,0,0))
        return reader,planner,parent,pointer,probe,corpus

    def test_saved_index_and_page_roots_publish_together_with_old_root_pinned(self):
        with evidence_case('paged-success') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root)
            old=ownership_pages.audit(parent['ownership_pages']);old_query=DeploymentReader(pointer,reader)
            try:
                output=root/'private'
                dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,output,probe,max_rows=128)
                record=load_deployment(pointer)
                self.assertEqual(record['tree']['source']['row_count'],18)
                self.assertEqual(record['ownership_pages']['global_count'],18)
                self.assertEqual(record['ownership_pages']['parent_sha256'],old.sha256)
                self.assertEqual(ownership_pages.lookup(old,'s0',7),7)
                self.assertEqual(len(old_query.query([0,0,0,0],16,beam_width=None,candidate_budget=16)['ids']),16)
                current=DeploymentReader(pointer,reader)
                try:self.assertEqual(set(current.query([0,0,0,0],18,beam_width=None,candidate_budget=18)['ids']),set(range(18)))
                finally:current.close()
                stats=json.loads((output/'ownership-page-verification.json').read_text())
                self.assertEqual(stats['reused_pages'],4);self.assertEqual(stats['changed_pages'],2)
                self.assertEqual(stats['verified_bytes'],16);self.assertEqual(stats['total_page_bytes'],144)
                self.assertEqual(stats['candidate_root_sha256'],content_hash(record['ownership_pages']))
                second_corpus=root/'append-again.f32';second_corpus.write_bytes(corpus.read_bytes()+struct.pack('<ffff',18,1,0,0))
                dynamic_write.publish_global_append(pointer,record,planner,reader,second_corpus,root/'second-private',probe,max_rows=128)
                second=load_deployment(pointer)
                self.assertEqual(second['ownership_pages']['global_count'],19)
                self.assertEqual(second['ownership_pages']['parent_sha256'],content_hash(record['ownership_pages']))
                self.assertEqual(ownership_pages.lookup(ownership_pages.audit(record['ownership_pages']),'s1',8),17)
                self.assertEqual(ownership_pages.lookup(old,'s1',7),15)
                (root/'publication-receipt.json').write_text(json.dumps(dict(parent_sha256=content_hash(parent),published_sha256=content_hash(record),root_sha256=content_hash(record['ownership_pages']),page_verification=stats),indent=2)+'\n')
            finally:old_query.close()

    def test_partial_page_write_after_real_owner_flush_blocks_publication_and_fresh_retry(self):
        with evidence_case('paged-partial') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root);head=pointer.read_bytes();output=root/'failed'
            original=ownership_pages._page;calls=[]
            def fail_second(*args):
                calls.append(1)
                if len(calls)==2:raise OSError('injected ownership page flush failure')
                return original(*args)
            with patch.object(ownership_pages,'_page',side_effect=fail_second):
                with self.assertRaisesRegex(OSError,'page flush failure'):
                    dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,output,probe,max_rows=128)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertTrue((output/'index-s0/manifest-2.json').exists());self.assertTrue((output/'index-s1/manifest-2.json').exists())
            self.assertEqual(len(list((output/'ownership-pages').iterdir())),1)
            self.assertEqual(json.loads(Path(str(output)+'.transaction.json').read_text())['status'],'failed_unpublished')
            dynamic_write.publish_global_append(pointer,parent,planner,reader,corpus,root/'fresh',probe,max_rows=128)
            self.assertEqual(load_deployment(pointer)['ownership_pages']['global_count'],18)

    def test_wrong_page_local_binding_and_stale_complete_root_rejected(self):
        with evidence_case('paged-stale') as root:
            reader,planner,parent,pointer,probe,corpus=self.prepare(root)
            candidate=dynamic_write.prepare_global_append(planner,reader,parent,corpus,root/'private',max_rows=128)
            bad=copy.deepcopy(candidate)
            a,b=bad['ownership_pages']['owners'];a['pages'],b['pages']=b['pages'],a['pages']
            with self.assertRaisesRegex(ValueError,'local offsets'):validate_record(bad)
            winner=make_record(parent['tree'],parent['owners'],parent)
            publish_deployment(pointer,winner,content_hash(parent),probe);head=pointer.read_bytes()
            with self.assertRaisesRegex(ValueError,'compare-and-swap'):publish_deployment(pointer,candidate,content_hash(parent),probe)
            self.assertEqual(pointer.read_bytes(),head)
            self.assertEqual(load_deployment(pointer)['ownership_pages'],parent['ownership_pages'])


if __name__=='__main__':unittest.main()
