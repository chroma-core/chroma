"""Pure protocol/route contracts; these tests do not start Rust or agents."""
import io
import queue
import threading
import time
from .dynamic_write_session import AgentWriteSessions
import unittest
from .dynamic_remote_write import RemoteWriteCoordinator, f32_bits, total_key
from .dynamic_write_session import PrivateWriteChannel


def node(parent, ident, leaf, distance):
    return dict(parent_id=parent,node_id=ident,is_leaf=leaf,distance=distance,distance_bits=f32_bits(distance))


class RemoteWriteContract(unittest.TestCase):
    def fixture(self, fail=False):
        calls=[]; journal=[]
        def call(owner, handle, command, row):
            calls.append((owner,command,row))
            op=command['op']
            if op=='root':return dict(op=op,root=node(0,0,False,0. if owner=='a' else 10.))
            if op=='expand':return dict(op=op,children=[node(0,1,True,3. if owner=='a' else -0. if row==18 else 0.)])
            if op=='count':return dict(op=op,count=2)
            if fail:raise EOFError('uncertain registration acknowledgement')
            return dict(op='registered',global_id=row,local_id=command['local_id'],leaf_id=command['leaf'],route_sha256=command['route_sha256'])
        info={o:dict(op='info',policy=dict(tau=1.,min=1,max=2,level_taus=[],level_min_pcts=[]),local_level_widths=[1,1],old_local_count=9) for o in ['a','b']}
        coordinator=RemoteWriteCoordinator(dict(a='s1',b='s2'),info,'frozen',dict(a=9,b=9),[dict(global_id=i,embedding_bits=[0]) for i in [18,19]],call,journal.append)
        return coordinator,calls,journal

    def test_actual_global_count_selects_across_upper_boundary_once(self):
        c,calls,journal=self.fixture();routes=c.run()
        self.assertEqual([r['subtree_id'] for r in routes],['b','b'])
        self.assertEqual([r['local_id'] for r in routes],[9,10])
        self.assertEqual(len([v for v in calls if v[1]['op']=='register']),2)
        self.assertTrue(all(v[1]['global_level_widths']==[2,2] for v in calls if v[1]['op']=='count'))
        self.assertLess(total_key(-0.),total_key(0.))
        with self.assertRaises(ValueError):c.run()

    def test_equal_distance_owner_tie_and_signed_zero_match_explicit_policy(self):
        c,_,_=self.fixture();original=c.call
        def tied(owner,handle,command,row):
            value=original(owner,handle,command,row)
            if command['op']=='expand':value['children']=[node(0,1,True,-0. if row==19 and owner=='b' else 0.)]
            return value
        c.call=tied
        self.assertEqual([r['subtree_id'] for r in c.run()],['a','b'])

    def test_uncertain_register_poisons_without_replay(self):
        c,calls,_=self.fixture(True)
        with self.assertRaises(EOFError):c.run()
        n=len(calls)
        with self.assertRaises(ValueError):c.run()
        self.assertEqual(len(calls),n)

    def test_every_child_is_validated_before_global_pruning(self):
        c,_,_=self.fixture();original=c.call
        def invalid(owner,handle,command,row):
            value=original(owner,handle,command,row)
            if command['op']=='expand' and owner=='a':value['children'].append(dict(parent_id=0,node_id=2,is_leaf=True,distance=float('inf'),distance_bits=0x7f800000))
            return value
        c.call=invalid
        with self.assertRaises(ValueError):c.run()

    def test_channel_wrong_scope_poison_prevents_later_write(self):
        c=PrivateWriteChannel.__new__(PrivateWriteChannel)
        c.closed=c.poisoned=c.terminal=False;c.sequence=0;c.timeout=.01
        c.spec=dict(session_id='s',attempt_id='a',authorization='auth',ownership_generation=2,base=dict(parent_sha256='p',owners=[dict(manifest_sha256='m')]),source_map_sha256='map')
        c.responses=queue.Queue();c.responses.put(b'{}\n')
        c.process=type('Process',(),dict(stdin=io.BytesIO()))()
        with self.assertRaises(ValueError):c.call(dict(op='info'))
        size=len(c.process.stdin.getvalue())
        with self.assertRaises(ValueError):c.call(dict(op='info'))
        self.assertEqual(len(c.process.stdin.getvalue()),size)


class WriterIsolationContract(unittest.TestCase):
    def test_blocked_pipe_write_is_deadline_covered_and_poisoned(self):
        release=threading.Event()
        class Pipe:
            def write(self, raw):release.wait(2);return len(raw)
            def flush(self):pass
        c=PrivateWriteChannel.__new__(PrivateWriteChannel);c.closed=c.poisoned=c.terminal=False;c.sequence=0;c.timeout=.02
        c.spec=dict(session_id='s',attempt_id='a',authorization='auth',ownership_generation=2,base=dict(parent_sha256='p',owners=[dict(manifest_sha256='m')]),source_map_sha256='map');c.responses=queue.Queue();c.process=type('Process',(),dict(stdin=Pipe()))()
        started=time.monotonic()
        try:
            with self.assertRaises(queue.Empty):c.call(dict(op='info'))
            self.assertTrue(c.poisoned);self.assertLess(time.monotonic()-started,.5)
        finally:
            release.set();c.writer_thread.join(1)
        self.assertFalse(c.writer_thread.is_alive())

    def test_concurrent_command_latches_failure_before_first_reply_is_eligible(self):
        entered=threading.Event();release=threading.Event();errors=[]
        class Channel:
            calls=0;closed=False
            def call(self,*args):self.calls+=1;entered.set();release.wait(1);return dict(op='root')
            def close(self):self.closed=True
        channel=Channel();row=dict(state='active',channel=channel,replica_id='replica',job_id='job',lock=threading.Lock(),failed=threading.Event())
        sessions=AgentWriteSessions.__new__(AgentWriteSessions);sessions.sessions={'handle':row};sessions.agent=type('Agent',(),dict(reservations={'replica':{} }))()
        payload=dict(handle='handle',job_id='job',command=dict(op='root'))
        def first():
            try:sessions.command(payload)
            except Exception as error:errors.append(error)
        thread=threading.Thread(target=first);thread.start()
        try:
            self.assertTrue(entered.wait(1))
            with self.assertRaisesRegex(ValueError,'one in-flight'):sessions.command(payload)
        finally:release.set();thread.join(1)
        self.assertFalse(thread.is_alive());self.assertEqual(channel.calls,1);self.assertTrue(channel.closed);self.assertTrue(errors);self.assertEqual(row['state'],'poisoned')
        with self.assertRaisesRegex(ValueError,'failed or unavailable'):sessions.status(payload)

    def test_registered_reply_rejects_bool_leaf_even_when_numerically_equal(self):
        c,_,_=RemoteWriteContract().fixture();original=c.call
        def malformed(owner,handle,command,row):
            result=original(owner,handle,command,row)
            if command['op']=='register':result['leaf_id']=True
            return result
        c.call=malformed
        with self.assertRaisesRegex(ValueError,'registration differs'):c.run()


class CleanupClassificationContract(unittest.TestCase):
    def test_denied_kill_is_sticky_unreconciled_and_never_retried(self):
        import subprocess
        class Process:
            returncode=None;waits=0;kills=0
            stdin=io.BytesIO();stdout=io.BytesIO()
            def wait(self,timeout):self.waits+=1;raise subprocess.TimeoutExpired('owned',timeout)
            def kill(self):self.kills+=1;raise PermissionError('mock denied kill')
        c=PrivateWriteChannel.__new__(PrivateWriteChannel);c.process=Process();c.stderr=io.BytesIO();c.timeout=.01;c.closed=False;c.poisoned=False;c.closing=False;c.cleanup_failure=None
        with self.assertRaisesRegex(ValueError,'unreconciled'):c.close()
        self.assertFalse(c.closed);self.assertFalse(c.closing);self.assertTrue(c.poisoned)
        with self.assertRaisesRegex(ValueError,'prior.*unreconciled'):c.close()
        self.assertEqual((c.process.waits,c.process.kills),(1,1))
        sessions=AgentWriteSessions.__new__(AgentWriteSessions);sessions.sessions={'h':dict(job_id='job',state='active',lock=threading.Lock(),failed=threading.Event(),channel=c)}
        with self.assertRaisesRegex(ValueError,'unreconciled'):sessions.close_one(dict(handle='h',job_id='job'))
        self.assertEqual(sessions.sessions['h']['state'],'cleanup-unreconciled');self.assertEqual(c.process.kills,1)

    def test_controller_rejects_closed_reply_without_actual_exit_code(self):
        from .dynamic_remote_write import RemoteWriteExecution
        class Client:
            def rpc(self,payload):return dict(handle='h',state='closed',started=True,returncode=None,threads_stopped=True)
        class Ledger:
            state=dict(operations=[],write_sessions={'s':dict(worker='w',handle='h')})
            def save(self):pass
        execution=RemoteWriteExecution.__new__(RemoteWriteExecution);execution.generation=None;execution.ledger=Ledger();execution.clients={'w':Client()};execution.context=dict(compaction=dict(job_id='job'))
        with self.assertRaisesRegex(ValueError,'unreconciled'):execution.close()
        self.assertNotIn('cleanup',execution.ledger.state['write_sessions']['s'])
