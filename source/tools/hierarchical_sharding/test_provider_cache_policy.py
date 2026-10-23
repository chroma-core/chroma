"""Optional existing-provider policy admission and retained-fixture parity."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from .dynamic_runtime import DeploymentReader
from .subtree_transport import PinnedReader, TreeError, validate_provider_cache_policy

SMALL=dict(block_capacity_units=1,root_capacity_entries=1,shards=1,clear_after_operation=False)
CLEAR=dict(SMALL,clear_after_operation=True)

class Policy(unittest.TestCase):
    def test_schema_units_and_shard_partition_are_strict_and_copied(self):
        self.assertIsNone(validate_provider_cache_policy(None))
        self.assertEqual(validate_provider_cache_policy(SMALL),SMALL)
        self.assertIsNot(validate_provider_cache_policy(SMALL),SMALL)
        for value in (dict(SMALL,block_capacity_units=True),dict(SMALL,shards=0),dict(SMALL,shards=3),dict(SMALL,shards=2),dict(SMALL,clear_after_operation=1),dict(SMALL,unknown=1),dict(SMALL,block_capacity_units=2**64)):
            with self.assertRaises(ValueError):validate_provider_cache_policy(value)
        with self.assertRaises(ValueError):DeploymentReader('/not-opened','/not-opened',backend=object(),provider_cache_policy=SMALL)

    def test_default_argv_is_unchanged_and_optional_argv_is_exact(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'manifest-1.json';path.write_text(json.dumps(dict(generation=1,shard_id='s0')))
            descriptor=dict(binary='/binary',checkpoint_dir=root,manifest=str(path),snapshot_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            with patch('tools.hierarchical_sharding.server.Reader') as child:
                PinnedReader(descriptor);self.assertNotIn('--provider-cache-policy',child.call_args.args[0])
                PinnedReader(dict(descriptor,provider_cache_policy=SMALL));argv=child.call_args.args[0]
                self.assertEqual(json.loads(argv[argv.index('--provider-cache-policy')+1]),SMALL)

    def test_silent_fallback_and_wrong_error_echo_are_rejected(self):
        pinned=PinnedReader.__new__(PinnedReader);pinned.descriptor={'snapshot_sha256':'hash'};pinned.snapshot={'generation':1,'shard_id':'s0'};pinned.provider_cache_policy=dict(SMALL);pinned.reader=Mock()
        for result in ({},{'error':'invalid query'},{'provider_cache_policy':CLEAR}):
            pinned.reader.query.return_value=result
            with self.assertRaisesRegex(TreeError,'provider cache policy'):pinned.call(dict(subtree_id='s0',snapshot_sha256='hash',payload=dict(op='describe_root')))

@unittest.skipUnless(os.environ.get('SPANN_PROVIDER_BINARY') and os.environ.get('SPANN_RETAINED_STREAM_POINTER'),'admitted read-only native epoch and retained fixture required')
class NativePolicy(unittest.TestCase):
    def test_repeated_full_traces_ties_clear_error_and_same_process_retry(self):
        from .test_dynamic_write import evidence_case
        from .test_dynamic_trie_query import logical_trace
        with evidence_case('provider-cache-full-query') as root:
            pointer=Path(os.environ['SPANN_RETAINED_STREAM_POINTER']);head=pointer.read_bytes();readers={};raw=[];proof=[]
            try:
                for mode,policy in (('default',None),('small',SMALL),('clear',CLEAR)):
                    reader=DeploymentReader(pointer,os.environ['SPANN_PROVIDER_BINARY'],timeout=5,worker_id_mode='local-only',evict_postings_after_operation=True,profile_memory=True,provider_cache_policy=policy);readers[mode]=reader
                    for sid,pinned in reader.coordinator.backend.readers.items():
                        original=pinned.reader.query
                        def record(payload,*args,_call=original,_mode=mode,_sid=sid,**kwargs):
                            result=_call(payload,*args,**kwargs);raw.append(dict(mode=_mode,subtree_id=_sid,request=payload,response=result));return result
                        pinned.reader.query=record
                for query,budget in (([4.5,1,0,0],1),([4.5,1,0,0],1),([8.5,1,0,0],16)):
                    results={mode:reader.query(query,min(4,budget),beam_width=None,candidate_budget=budget) for mode,reader in readers.items()}
                    for result in results.values():self.assertEqual(logical_trace(result),logical_trace(results['default']))
                    proof.append(dict(query=query,budget=budget,results=results))
                for mode,reader in readers.items():
                    for sid,pinned in reader.coordinator.backend.readers.items():
                        invalid=pinned.reader.query(dict(op='score_selected_leaves',query=[0],nodes=[0],candidate_budget=1,generation=pinned.snapshot['generation'],request_id='invalid'))
                        self.assertIn('error',invalid);self.assertEqual(invalid.get('provider_cache_policy'),None if mode=='default' else SMALL if mode=='small' else CLEAR)
                    retry=reader.query([4.5,1,0,0],1,beam_width=None,candidate_budget=1)
                    self.assertEqual(logical_trace(retry),logical_trace(proof[0]['results']['default']))
                self.assertEqual(pointer.read_bytes(),head)
            finally:
                for reader in readers.values():reader.close()
                (root/'raw.json').write_text(json.dumps(dict(queries=proof,raw_operations=raw,scope='cache weighted targets and explicit lifecycle only; no RSS/decode bound'),indent=2)+'\n')
