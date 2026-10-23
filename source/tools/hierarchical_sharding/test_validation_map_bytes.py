"""Checked bytes stay private to one complete validation call."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
from .dynamic_deployment import (_ValidationMaps, validate_record,
                                 validate_ownership_pages, validate_ownership_trie)


class OwnedComparisonMaps(unittest.TestCase):
    def test_consumed_digest_and_fresh_context_reject_same_size_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'map';raw=(1).to_bytes(8,'little');path.write_bytes(raw)
            subtree=dict(subtree_id='a',vector_count=1,local_to_global=dict(path=str(path),bytes=8,sha256=hashlib.sha256(raw).hexdigest()))
            scope=_ValidationMaps();self.assertEqual(scope.get(subtree),raw)
            path.write_bytes((2).to_bytes(8,'little'))
            self.assertEqual(scope.get(subtree),raw)
            with self.assertRaisesRegex(ValueError,'digest or length'):_ValidationMaps().get(subtree)
            self.assertEqual(scope.owned_bytes,8)

    def test_each_descriptor_owner_count_binding_and_budget_fail_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'map';raw=b'\0'*8;path.write_bytes(raw)
            subtree=dict(subtree_id='a',vector_count=1,local_to_global=dict(path=str(path),bytes=8,sha256=hashlib.sha256(raw).hexdigest()))
            scope=_ValidationMaps();scope.get(subtree)
            for field,value in [('path',str(path)+'-other'),('sha256','0'*64)]:
                changed=copy.deepcopy(subtree);changed['local_to_global'][field]=value
                with self.assertRaisesRegex(ValueError,'changed within'):scope.get(changed)
            for value in (True,0,100001):
                changed=copy.deepcopy(subtree);changed['vector_count']=value
                with self.assertRaises(ValueError):scope.get(changed)
            changed=copy.deepcopy(subtree);changed['local_to_global']['bytes']=True
            with self.assertRaises(ValueError):scope.get(changed)
            changed=copy.deepcopy(subtree);changed['vector_count']=2;changed['local_to_global']['bytes']=16
            with self.assertRaisesRegex(ValueError,'changed within'):scope.get(changed)
            scope=_ValidationMaps();scope.owned_bytes=800000
            with patch.object(Path,'open') as opened:
                with self.assertRaisesRegex(ValueError,'budget'):scope.get(subtree)
                opened.assert_not_called()

    def test_changed_length_after_stat_fails_consumed_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'map';raw=b'\0'*8;path.write_bytes(raw)
            subtree=dict(subtree_id='a',vector_count=1,local_to_global=dict(path=str(path),bytes=8,sha256=hashlib.sha256(raw).hexdigest()))
            for consumed in (raw[:-1],raw+b'\0'):
                with patch.object(Path,'open',return_value=io.BytesIO(consumed)):
                    with self.assertRaisesRegex(ValueError,'digest or length'):_ValidationMaps().get(subtree)


@unittest.skipUnless(os.environ.get('SPANN_VALIDATION_FIXTURE'),'retained fixture required')
class CompleteValidationFreshness(unittest.TestCase):
    def test_report_standalone_oracles_and_later_corruption_rejection(self):
        original=Path(os.environ['SPANN_VALIDATION_FIXTURE']).resolve()
        pointer=json.loads((original/'current.json').read_bytes())
        record=json.loads((original/pointer['manifest']).read_bytes())
        expected=validate_record(record)
        with tempfile.TemporaryDirectory() as directory:
            copied=Path(directory).resolve()/'fixture';shutil.copytree(original,copied)
            def rebase(value):
                if isinstance(value,dict):return {k:rebase(v) for k,v in value.items()}
                if isinstance(value,list):return [rebase(v) for v in value]
                if isinstance(value,str) and value.startswith(str(original)+'/'):return str(copied)+value[len(str(original)):]
                return value
            candidate=rebase(record)
            candidate['tree']=copy.deepcopy(record['tree'])
            actual=validate_record(candidate)
            # Logical identity uses original path descriptors; rebasing changes
            # that hash, so this fixture keeps the canonical logical tree pins
            # and resolves only physical owner/canonical/page/trie locations.
            self.assertEqual(actual,expected)
            validate_ownership_pages(candidate);validate_ownership_trie(candidate)
            owner=next(iter(candidate['owners'].values()));path=Path(owner['checkpoint_dir'])/'id-map-1.u64le'
            old=path.read_bytes();stamp=path.stat();path.chmod(0o600);path.write_bytes(bytes([old[0]^1])+old[1:]);os.utime(path,ns=(stamp.st_atime_ns,stamp.st_mtime_ns))
            failures=[]
            for function in (validate_record,validate_ownership_pages,validate_ownership_trie):
                with self.assertRaises(ValueError):function(candidate)
                failures.append(function.__name__)
            if os.environ.get('SPANN_VALIDATION_EVIDENCE'):
                output=Path(os.environ['SPANN_VALIDATION_EVIDENCE']);output.mkdir(exist_ok=False,parents=True)
                (output/'freshness.json').write_text(json.dumps(dict(report=actual,standalone_passed=True,later_same_size_corruption_rejected=failures),indent=2)+'\n')
