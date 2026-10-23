"""Offline adversarial receipt tests; no Rust, sockets or process signals."""
import copy
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
from .private_write_outcome import CLASSIFICATION,command_identity,validate_discarded
from .dynamic_write_session import AgentWriteSessions
from .tree_manifest import canonical_bytes

class PrivateDiscardContract(unittest.TestCase):
    def fixture(self,root):
        scope=dict(job_id='job',session_id='session',handle='handle',replica_id='replica',spec_sha256='a'*64)
        command=dict(op='selected-write-command',handle='handle',job_id='job',operation_id='b'*32,command=dict(op='register',leaf=7),row_id=18)
        identity=command_identity(command)
        class Channel:
            closed=True
            process=type('Process',(),dict(returncode=1))()
            def close(self):pass
        agent=type('Agent',(),{})();agent.lock=threading.RLock();agent.reservations={'replica':dict(operation='selected-write',state='failed')};agent.replicas={};agent.generations={}
        manager=AgentWriteSessions(agent);row=dict(state='poisoned',channel=Channel(),replica_id='replica',job_id='job',lock=threading.Lock(),failed=threading.Event(),inputs=Path(root),command_scope=scope,commands=[identity],command_journal=Path(root)/'intent.json',commit_started=False,intent_persistence_failed=False)
        row['failed'].set();manager.sessions['handle']=row;manager._persist_command_intents(row)
        return manager,row,dict(identity,kind='selected-write-command',**scope),dict(handle='handle',replica_id='replica',spec_sha256='a'*64)

    def test_terminal_discard_preserves_unknown_effects_and_rejects_scope_changes(self):
        with tempfile.TemporaryDirectory() as root:
            manager,row,operation,session=self.fixture(root)
            close=manager.close_one(dict(handle='handle',job_id='job'));self.assertEqual(close['returncode'],1)
            receipt=manager.outcome(dict(handle='handle',job_id='job'))
            self.assertEqual(validate_discarded(receipt,operation,session,'job','session'),CLASSIFICATION)
            for field,value in [('job_id','other'),('session_id','other'),('handle','other'),('replica_id','other'),('spec_sha256','c'*64)]:
                wrong=copy.deepcopy(receipt);wrong['scope'][field]=value
                with self.assertRaises(ValueError):validate_discarded(wrong,operation,session,'job','session')
            for field in ['job_id','session_id','handle','replica_id','spec_sha256']:
                wrong_operation=dict(operation);wrong_operation[field]='other'
                with self.assertRaises(ValueError):validate_discarded(receipt,wrong_operation,session,'job','session')
            wrong=dict(operation,payload_sha256='c'*64)
            with self.assertRaises(ValueError):validate_discarded(receipt,wrong,session,'job','session')
            self.assertEqual(row['state'],'closed');self.assertTrue(row['failed'].is_set())

    def test_commit_persistence_failure_or_eligible_namespace_never_gets_receipt(self):
        for kind in ['commit','persistence','eligible','generation','active','changed-intent']:
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as root:
                manager,row,_,_=self.fixture(root)
                if kind=='commit':row['commit_started']=True;manager._persist_command_intents(row)
                if kind=='persistence':row['intent_persistence_failed']=True
                if kind=='eligible':manager.agent.replicas['replica']={}
                if kind=='generation':manager.agent.generations['g']=dict(replica_ids={'replica'})
                if kind=='active':row['failed'].clear()
                if kind=='changed-intent':row['command_journal'].write_bytes(canonical_bytes({}))
                if kind=='changed-intent':
                    with self.assertRaises(ValueError):manager.close_one(dict(handle='handle',job_id='job'))
                else:manager.close_one(dict(handle='handle',job_id='job'))
                with self.assertRaises(ValueError):manager.outcome(dict(handle='handle',job_id='job'))

    def test_intent_flush_error_latches_uncertainty(self):
        with tempfile.TemporaryDirectory() as root:
            manager,row,_,_=self.fixture(root)
            with patch('tools.hierarchical_sharding.tree_manifest._atomic_write',side_effect=OSError('declared test failure')):
                with self.assertRaises(OSError):manager._persist_command_intents(row)
            self.assertTrue(row['intent_persistence_failed'])
            manager.close_one(dict(handle='handle',job_id='job'))
            with self.assertRaises(ValueError):manager.outcome(dict(handle='handle',job_id='job'))

    def test_commit_latch_is_durable_before_native_forward(self):
        with tempfile.TemporaryDirectory() as root:
            manager,row,_,_=self.fixture(root);row['state']='active';row['failed'].clear();row['commands']=[];manager._persist_command_intents(row)
            payload=dict(op='selected-write-command',handle='handle',job_id='job',operation_id='d'*32,command=dict(op='commit',routes=[]),row_id=None)
            def forward(command,row_id):
                state=json.loads(row['command_journal'].read_bytes())
                self.assertIs(state['commit_started'],True);self.assertEqual(state['commands'],[command_identity(payload)])
                raise EOFError('unknown commit reply')
            row['channel'].call=forward
            with self.assertRaises(EOFError):manager.command(payload)
            self.assertTrue(row['failed'].is_set());self.assertTrue(row['commit_started'])
            manager.close_one(dict(handle='handle',job_id='job'))
            with self.assertRaises(ValueError):manager.outcome(dict(handle='handle',job_id='job'))

    def test_controller_only_resolves_bound_failed_pre_candidate_after_close(self):
        from .dynamic_remote_write import RemoteWriteExecution
        with tempfile.TemporaryDirectory() as root:
            manager,row,operation,session=self.fixture(root);manager.close_one(dict(handle='handle',job_id='job'))
            receipt=manager.outcome(dict(handle='handle',job_id='job'));operation.update(status='unreconciled',worker='worker');session.update(worker='worker',cleanup='closed')
            execution=RemoteWriteExecution.__new__(RemoteWriteExecution);execution.poisoned=True;execution.generation=None;execution.context=dict(compaction=dict(job_id='job',session_id='session'))
            class Ledger:
                state=dict(preparation_state='failed-before-candidate',operations=[operation],write_sessions=dict(owner=session))
                def save(self):pass
            execution.ledger=Ledger();calls=[]
            class Client:
                def rpc(self,payload):calls.append(payload);return receipt
            execution.clients={'worker':Client()}
            self.assertEqual(execution.reconcile_discarded_private()['operations'],1)
            self.assertEqual(operation['status'],CLASSIFICATION);self.assertEqual([c['op'] for c in calls],['selected-write-outcome'])
            operation['status']='unreconciled';operation.pop('payload_sha256');calls.clear()
            with self.assertRaises(ValueError):execution.reconcile_discarded_private()
            self.assertEqual(calls,[]);self.assertEqual(operation['status'],'unreconciled')
            operation['payload_sha256']='e'*64
            with self.assertRaises(ValueError):execution.reconcile_discarded_private()
            self.assertEqual(operation['status'],'unreconciled')
            execution.ledger.state['preparation_state']='candidate-returned';calls.clear()
            with self.assertRaises(ValueError):execution.reconcile_discarded_private()
            self.assertEqual(calls,[])

    def test_controller_reserves_exact_command_payload_before_rpc(self):
        from .compactor_remote_topology import TransportLedger,TrackedClient
        with tempfile.TemporaryDirectory() as root:
            ledger=TransportLedger(Path(root)/'ledger',dict(fence=1))
            ledger.state['write_sessions']=dict(owner=dict(handle='handle',worker='worker',job_id='job',session_id='session',replica_id='replica',spec_sha256='a'*64));ledger.save()
            class Client:
                worker_id='worker'
                def rpc(inner,payload):
                    self.assertEqual(len(ledger.state['operations']),1);intent=ledger.state['operations'][0]
                    self.assertEqual(intent['status'],'started');self.assertEqual(intent['payload_sha256'],command_identity(payload)['payload_sha256'])
                    self.assertEqual(json.loads(ledger.path.read_bytes())['operations'][0],intent)
                    raise EOFError('lost acknowledgement')
            client=TrackedClient(Client(),ledger)
            with self.assertRaises(EOFError):client.rpc(dict(op='selected-write-command',handle='handle',job_id='job',command=dict(op='register',leaf=7),row_id=18))
            self.assertEqual(ledger.state['operations'][0]['status'],'unreconciled')

    def test_tampered_terminal_receipt_rejects_at_agent_and_controller(self):
        with tempfile.TemporaryDirectory() as root:
            manager,row,operation,session=self.fixture(root);manager.close_one(dict(handle='handle',job_id='job'))
            path=row['discard_receipt'];original=json.loads(path.read_bytes())
            from .private_write_outcome import TERMINAL_FIELDS
            for key,value in TERMINAL_FIELDS.items():
                with self.subTest(key=key):
                    bad=copy.deepcopy(original);bad[key]=not value if type(value) is bool else 'other'
                    path.write_bytes(canonical_bytes(bad))
                    with self.assertRaises(ValueError):manager.outcome(dict(handle='handle',job_id='job'))
                    with self.assertRaises(ValueError):validate_discarded(bad,operation,session,'job','session')
            path.write_bytes(canonical_bytes(original))
            self.assertEqual(manager.outcome(dict(handle='handle',job_id='job')),original)
