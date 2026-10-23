"""The same audit's owned bytes replace a redundant mutable-path reread."""
import json
import os
from pathlib import Path
import tempfile
import unittest
from . import ownership_pages as pages
from .validation_profile import ValidationIO

class OwnedAudit(unittest.TestCase):
    def test_owned_bytes_and_repeated_actual_audit_reject_mutated_file(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pages.create({'a':[0,2],'b':[1,3]},Path(folder).resolve()/'pages',1)
            receipt,owned=pages.audit_with_page_bytes(root)
            self.assertEqual(owned['a'],b'\0'*8+(2).to_bytes(8,'little'))
            path=Path(root['owners'][0]['pages'][0]['path']);path.write_bytes((3).to_bytes(8,'little'))
            self.assertEqual(owned['a'][:8],b'\0'*8)
            with self.assertRaises(ValueError):pages.audit_with_page_bytes(root)
            with self.assertRaises(ValueError):pages.audit(root)
    def test_default_receipt_matches_owned_receipt_and_empty_owner_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=pages.create({'a':[],'b':[0]},Path(folder).resolve()/'pages',2)
            receipt,owned=pages.audit_with_page_bytes(root)
            self.assertEqual(receipt,pages.audit(root));self.assertEqual(owned['a'],b'')

@unittest.skipUnless(os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native fixture builder required')
class RealPageAdmission(unittest.TestCase):
    def test_same_fixture_full_oracle_parity_read_reduction_and_later_corruption(self):
        from .test_dynamic_write import make_parent,evidence_case
        from .dynamic_deployment import validate_ownership_pages,validate_record,resolved_tree
        from .tree_manifest import checked_artifact
        with evidence_case('validation-owned-pages') as root:
            parent,_,_,_=make_parent(root,os.environ['SPANN_DYNAMIC_BINARY'],page_rows=4,trie_fanout=4)
            # Preserve the exact reference algorithm as the independent oracle.
            def reference():
                trusted=pages.audit(parent['ownership_pages']);tree=resolved_tree(parent);slots={s['subtree_id']:s for s in tree['subtrees']}
                for owner in parent['ownership_pages']['owners']:
                    with checked_artifact(slots[owner['owner']]['local_to_global'],'.').open('rb') as flat:
                        for page in owner['pages']:self.assertEqual(flat.read(page['bytes']),pages._read(page))
                        self.assertEqual(flat.read(1),b'')
                return trusted
            with ValidationIO() as before:expected=reference()
            with ValidationIO() as after:actual=validate_ownership_pages(parent)
            self.assertEqual(actual,expected);validate_record(parent)
            scope=str(root/'initial-ownership-pages')
            payload=lambda io:sum(v['read_bytes'] for path,v in io.report()['paths'].items() if path.startswith(scope+'/'))
            self.assertEqual(payload(before),256);self.assertEqual(payload(after),128)
            page=parent['ownership_pages']['owners'][0]['pages'][0];path=Path(page['path']);old=path.stat();data=path.read_bytes();path.write_bytes(bytes([data[0]^1])+data[1:]);os.utime(path,ns=(old.st_atime_ns,old.st_mtime_ns))
            with self.assertRaises(ValueError):validate_ownership_pages(parent)
            with self.assertRaises(ValueError):validate_record(parent)
            (root/'same-call-page-proof.json').write_text(json.dumps(dict(parent=parent,before=before.report(),after=after.report(),page_read_bytes_before=256,page_read_bytes_after=128,returned_receipt_equal=True,full_record_pass_before_corruption=True,later_same_size_mtime_corruption_rejected=True,scope='same-call owned bytes only; full later audit and all durability unchanged'),indent=2)+'\n')
