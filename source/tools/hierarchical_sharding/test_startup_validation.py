"""Reuse only the trie receipt from this constructor's complete validation."""
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch
from . import ownership_trie as trie
from .dynamic_runtime import DeploymentReader
from .test_dynamic_write import make_parent,evidence_case


@unittest.skipUnless(os.environ.get('SPANN_LOCAL_ONLY_BINARY') and os.environ.get('SPANN_DYNAMIC_BINARY'),'admitted native readers required')
class StartupValidation(unittest.TestCase):
    def fixture(self,root):
        return make_parent(root,os.environ['SPANN_DYNAMIC_BINARY'],page_rows=4,trie_fanout=4)

    def test_constructor_performs_one_full_trie_oracle_before_using_its_receipt(self):
        with evidence_case('single-startup-audit') as root:
            _,pointer,_,_=self.fixture(root);original=trie.audit_with_maps;calls=[]
            def observe(value):
                receipt,metrics,maps=original(value);calls.append((receipt,metrics));return receipt,metrics,maps
            with patch.object(trie,'audit_with_maps',side_effect=observe):
                reader=DeploymentReader(pointer,os.environ['SPANN_LOCAL_ONLY_BINARY'],worker_id_mode='local-only')
            try:
                self.assertEqual(len(calls),1)
                self.assertEqual(reader.id_mapper.root,calls[0][0].root())
                reader.coordinator.verify_roots()
                (root/'validation-receipt.json').write_text(json.dumps(dict(full_audit_calls=1,root_sha256=calls[0][0].sha256,full_audit_metrics=calls[0][1],mapper_metrics=reader.mapping_metrics()),indent=2)+'\n')
            finally:reader.close()

    def test_later_constructor_revalidates_actual_files_and_rejects_corruption_before_workers(self):
        with evidence_case('startup-corruption-rejection') as root:
            _,pointer,_,_=self.fixture(root)
            reader=DeploymentReader(pointer,os.environ['SPANN_LOCAL_ONLY_BINARY'],worker_id_mode='local-only')
            mapper=reader.id_mapper;record=mapper.store.lookup(mapper.root['directory'],0,0,trie._depth(128,4));page=mapper.store.lookup(record['pages'],0,0,trie._depth(trie.flat.MAX_ROWS,4));reader.close()
            path=Path(page['path']);path.chmod(0o600);data=path.read_bytes();path.write_bytes(b'corrupt!'+data[8:])
            with patch('tools.hierarchical_sharding.subtree_transport.LocalBackend') as workers:
                with self.assertRaisesRegex(ValueError,'digest or length'):
                    DeploymentReader(pointer,os.environ['SPANN_LOCAL_ONLY_BINARY'],worker_id_mode='local-only')
                workers.assert_not_called()
            (root/'corruption-receipt.json').write_text(json.dumps(dict(page=page,corruption='owned fixture page content changed without changing descriptor',new_constructor_rejected_before_worker_start=True),indent=2)+'\n')


if __name__=='__main__':unittest.main()
