"""Bounded ownership-page correctness checks against complete flat-map audits."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from . import ownership_pages as pages
from .tree_manifest import canonical_bytes


class OwnershipPages(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        self.before=pages.create({'a':[0,2,4],'b':[1,3],'empty':[]},self.root/'initial',2)
        self.trusted=pages.audit(self.before)

    def test_boundary_empty_owner_and_repeated_append_match_full_oracle(self):
        candidate=pages.append(self.trusted,{'a':[5,7],'empty':[6]},self.root/'append')
        stats=pages.verify_append(self.trusted,candidate)
        self.assertEqual(stats,dict(changed_pages=3,reused_pages=2,verified_bytes=40,new_rows=3))
        self.assertEqual(pages.audit(candidate).sha256,pages.content_hash(candidate))
        incremental,verified=pages.audit_append(self.trusted,candidate)
        self.assertEqual(incremental.sha256,pages.audit(candidate).sha256)
        next_root=pages.append(incremental,{'b':[8]},self.root/'next')
        self.assertEqual(next_root['owners'][0],candidate['owners'][0])
        self.assertEqual(pages.audit(next_root).root()['global_count'],9)
        self.assertEqual(pages.audit(self.before).sha256,self.trusted.sha256)
        self.assertEqual(pages.lookup(self.trusted,'a',2),4)
        self.assertEqual(pages.lookup(incremental,'a',4),7)
        with self.assertRaises(ValueError):pages.lookup(self.trusted,'a',3)

    def test_incremental_verifier_reads_only_changed_pages_and_old_tail(self):
        candidate=pages.append(self.trusted,{'empty':[5]},self.root/'append')
        actual=pages._read;reads=[]
        def tracked(page):reads.append(page['path']);return actual(page)
        with patch.object(pages,'_read',side_effect=tracked):stats=pages.verify_append(self.trusted,candidate)
        self.assertEqual(len(reads),1);self.assertEqual(stats['verified_bytes'],8)
        self.assertEqual(stats['reused_pages'],3)

    def test_malformed_root_and_reassigned_old_prefix_rejected(self):
        candidate=pages.append(self.trusted,{'a':[5]},self.root/'append')
        bad=copy.deepcopy(candidate);page=bad['owners'][0]['pages'][-1]
        replacement=self.root/'bad.u64le';replacement.write_bytes(pages.struct.pack('<QQ',3,5))
        page.update(path=str(replacement),sha256=pages.hashlib.sha256(replacement.read_bytes()).hexdigest())
        with self.assertRaisesRegex(ValueError,'old local prefix'):pages.verify_append(self.trusted,bad)
        for mutate in [lambda r:r['owners'][0]['pages'][0].update(start=True),lambda r:r['owners'][0]['pages'][0].update(path='/owned/../outside'),lambda r:r.update(global_count=True),lambda r:r['owners'][0]['pages'].append(r['owners'][0]['pages'][0]),lambda r:r['owners'][0]['pages'][0].update(sha256='0'*64)]:
            bad=copy.deepcopy(candidate);mutate(bad)
            with self.assertRaises(ValueError):pages.verify_append(self.trusted,bad)

    def test_reserved_global_range_rejects_duplicates_gaps_and_old_ids_before_writes(self):
        for additions in [{'a':[5],'b':[5]},{'a':[6]},{'empty':[0]},{'unknown':[5]}]:
            destination=self.root/'must-not-exist'
            with self.assertRaises(ValueError):pages.append(self.trusted,additions,destination)
            self.assertFalse(destination.exists())

    def test_partial_page_write_failure_leaves_no_publishable_root_and_fresh_retry(self):
        pointer=self.root/'current.json';pointer.write_bytes(canonical_bytes(self.before))
        original=pages._page;calls=[]
        def failure(*args):
            calls.append(1)
            if len(calls)==2:raise OSError('injected page flush failure')
            return original(*args)
        with patch.object(pages,'_page',side_effect=failure):
            with self.assertRaises(OSError):pages.append(self.trusted,{'a':[5],'empty':[6]},self.root/'failed')
        self.assertTrue(list((self.root/'failed').iterdir()));self.assertEqual(json.loads(pointer.read_bytes()),self.before)
        with self.assertRaises(FileExistsError):pages.append(self.trusted,{'a':[5]},self.root/'failed')
        candidate=pages.append(self.trusted,{'a':[5],'empty':[6]},self.root/'fresh')
        pages.publish(pointer,candidate,self.trusted)
        self.assertEqual(pages.audit(json.loads(pointer.read_bytes())).root()['global_count'],7)

    def test_stale_root_cas_rejects_complete_candidate(self):
        pointer=self.root/'current.json';pointer.write_bytes(canonical_bytes(self.before))
        winner=pages.append(self.trusted,{'a':[5]},self.root/'winner')
        loser=pages.append(self.trusted,{'b':[5]},self.root/'loser')
        pages.publish(pointer,winner,self.trusted);head=pointer.read_bytes()
        with self.assertRaisesRegex(ValueError,'compare-and-swap'):pages.publish(pointer,loser,self.trusted)
        self.assertEqual(pointer.read_bytes(),head)
        self.assertEqual(pages.audit(self.before).sha256,self.trusted.sha256)

    def test_topology_replacement_requires_exact_parent_and_complete_live_set(self):
        replacement=pages.replace(self.trusted,{'merged':[0,1,2,3,4]},self.root/'replacement')
        self.assertEqual(pages.verify_replacement(self.trusted,replacement).root()['global_count'],5)
        bad=copy.deepcopy(replacement);bad['parent_sha256']='0'*64
        with self.assertRaises(ValueError):pages.verify_replacement(self.trusted,bad)
        for maps in [{'merged':[0,1,2,3]},{'merged':[0,1,2,3,3]},{'merged':[0,1,2,3,5]}]:
            with self.assertRaises(ValueError):pages.replace(self.trusted,maps,self.root/'must-not-exist')
            self.assertFalse((self.root/'must-not-exist').exists())


if __name__=='__main__':unittest.main()
