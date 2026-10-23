"""Descriptor-path, cache, immutable-parent and changed-path proof tests."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from . import ownership_trie as trie
from . import ownership_pages as flat


class DescriptorPaths(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()

    def test_lookup_path_reads_do_not_grow_with_page_count_and_cache_is_bounded(self):
        readings=[]
        for count in (16,256):
            root=trie.create({'a':list(range(count)),'empty':[]},self.root/str(count),page_rows=4)
            receipt,_=trie.audit(root);reader=trie.PinnedReader(receipt)
            self.assertEqual(reader.lookup('a',count-1),count-1)
            first=reader.store.metrics['metadata_disk_reads'];readings.append(first)
            self.assertEqual(reader.lookup('a',count-1),count-1)
            self.assertEqual(reader.store.metrics['metadata_disk_reads'],first)
            self.assertEqual(reader.store.metrics['metadata_parses'],first)
            self.assertLessEqual(reader.store.metrics['peak_cache_nodes'],32)
            self.assertLessEqual(reader.store.metrics['peak_cache_owned_bytes'],262144)
            bounded=trie.PinnedReader(receipt,cache_nodes=2,cache_bytes=8192)
            for local in (0,count-1,1):self.assertEqual(bounded.lookup('a',local),local)
            self.assertLessEqual(bounded.store.metrics['peak_cache_nodes'],2)
            self.assertLessEqual(bounded.store.metrics['peak_cache_owned_bytes'],8192)
        self.assertEqual(readings,[13,13])

    def test_partial_tail_empty_owner_repeated_append_and_changed_path_counts(self):
        root=trie.create({'a':[0,1,2],'empty':[]},self.root/'initial',page_rows=4)
        parent,_=trie.audit(root);old_bytes=parent.encoded
        candidate,trusted,stats=trie.append(parent,{'a':[3,5],'empty':[4]},self.root/'append')
        self.assertEqual(trie.audit(candidate)[0].sha256,trusted.sha256)
        reader=trie.PinnedReader(trusted)
        self.assertEqual([reader.lookup('a',i) for i in range(5)],[0,1,2,3,5]);self.assertEqual(reader.lookup('empty',0),4)
        next_root,next_trust,next_stats=trie.append(trusted,{'empty':[6]},self.root/'again')
        self.assertEqual(trie.PinnedReader(next_trust).lookup('empty',1),6)
        self.assertEqual(trie.audit(next_root)[0].sha256,next_trust.sha256)
        self.assertEqual(trie.PinnedReader(parent).lookup('a',2),2)
        self.assertEqual(parent.encoded,old_bytes);self.assertEqual(trie.audit(root)[0].sha256,parent.sha256)
        self.assertEqual(stats['new_rows'],3);self.assertEqual(next_stats['changed_pages'],1)
        with self.assertRaises(ValueError):trie.PinnedReader(parent).lookup('a',3)

    def test_single_append_reads_changed_paths_instead_of_all_metadata(self):
        root=trie.create({'a':list(range(256))},self.root/'initial',page_rows=4)
        parent,full=trie.audit(root)
        candidate,_,stats=trie.append(parent,{'a':[256]},self.root/'append')
        self.assertLess(stats['metadata_disk_reads'],full['metadata_disk_reads'])
        self.assertLessEqual(stats['metadata_disk_reads'],26)
        self.assertEqual(stats['page_read_bytes'],8)
        self.assertEqual(stats['changed_pages'],1)
        self.assertEqual(trie.audit(candidate)[0].root()['global_count'],257)

    def test_corruption_wrong_ranges_counts_and_stale_root_fail_closed(self):
        root=trie.create({'a':[0,1,2,3]},self.root/'initial',page_rows=2)
        parent,_=trie.audit(root);candidate,_,_=trie.append(parent,{'a':[4]},self.root/'append')
        stale=copy.deepcopy(candidate);stale['parent_sha256']='0'*64
        with self.assertRaises(ValueError):trie.verify_append(parent,stale)
        for mutate in [lambda d:d.update(start=1),lambda d:d.update(count=True),lambda d:d.update(bytes=65537),lambda d:d.update(path='/owned/../outside'),lambda d:d.update(sha256='0'*64)]:
            malformed=copy.deepcopy(root);mutate(malformed['directory'])
            with self.assertRaises(ValueError):trie.audit(malformed)
        path=Path(candidate['directory']['path']);path.chmod(0o600);path.write_bytes(path.read_bytes()+b' ')
        with self.assertRaises(ValueError):trie.verify_append(parent,candidate)
        self.assertEqual(trie.audit(root)[0].sha256,parent.sha256)

    def test_injected_owner_or_child_reference_and_oversized_page_rejected(self):
        root=trie.create({'a':[0,1]},self.root/'initial',page_rows=2)
        receipt,_=trie.audit(root);reader=trie.PinnedReader(receipt)
        owner=reader.store.lookup(root['directory'],0,0,trie._depth(128,4))
        malformed=copy.deepcopy(root);node=json.loads(Path(root['directory']['path']).read_bytes())
        first=next(iter(node['slots']));node['slots'][first]['start']+=1
        bad_path=self.root/'bad-node.json';bad_path.write_bytes(trie.canonical_bytes(node))
        malformed['directory'].update(path=str(bad_path),sha256=trie.hashlib.sha256(bad_path.read_bytes()).hexdigest(),bytes=bad_path.stat().st_size)
        with self.assertRaises(ValueError):trie.audit(malformed)
        page=reader.store.lookup(owner['pages'],0,0,trie._depth(flat.MAX_ROWS,4))
        path=Path(page['path']);path.chmod(0o600);path.write_bytes(path.read_bytes()+b'oversized')
        with patch.object(Path,'read_bytes',side_effect=AssertionError('unbounded page read')):
            with self.assertRaisesRegex(ValueError,'length differs'):flat._read(page)

    def test_partial_node_write_retained_and_fresh_attempt_succeeds(self):
        root=trie.create({'a':[0,1]},self.root/'initial',page_rows=2)
        parent,_=trie.audit(root);original=trie.Store.put;calls=[]
        def fail(store,*args):
            calls.append(1)
            if len(calls)==2:raise OSError('descriptor flush failure')
            return original(store,*args)
        with patch.object(trie.Store,'put',new=fail):
            with self.assertRaises(OSError):trie.append(parent,{'a':[2]},self.root/'failed')
        self.assertTrue(list((self.root/'failed').iterdir()))
        with self.assertRaises(FileExistsError):trie.append(parent,{'a':[2]},self.root/'failed')
        candidate,trusted,_=trie.append(parent,{'a':[2]},self.root/'fresh')
        self.assertEqual(trie.audit(candidate)[0].sha256,trusted.sha256)
        self.assertEqual(trie.audit(root)[0].sha256,parent.sha256)


if __name__=='__main__':unittest.main()
