"""Regressions for observed failure windows; no cloud or Rust required."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .guarded_host_batch import run_batch, remote_control
from .resource_guard import stop_groups


class RepairTests(unittest.TestCase):
    def test_failure_latches_before_abort_controls_and_terminal_reconciliation(self):
        events=[]
        terminal=dict(status='failed',reconciled=True,guard_report={})
        polls=0
        def control(action,entry,timeout):
            nonlocal polls
            if action=='launch':return dict(status='launched',identity={'pid':1},commands=1,lease_ttl_s=30)
            if action=='poll':
                polls+=1
                if polls==1:raise OSError('lost control')
                return dict(status='terminal',terminal=terminal)
            if action=='abort':
                self.assertIn('latched',events)
                events.append('abort')
            return {}
        with tempfile.TemporaryDirectory() as temp:
            result=run_batch(dict(hosts=[dict(host='a',output_path='/unused')],control_timeout_s=1),Path(temp)/'batch',control,sleep=lambda _:None,on_failure=lambda reason:events.append('latched'))
        self.assertFalse(result['publication_allowed']);self.assertEqual(events[0],'latched')

    def test_poll_rejects_revoked_lease_before_terminal_exists(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);lease=root/'lease';lease.write_text(json.dumps(dict(run_id='r',renewed_unix=0)))
            config=root/'config';config.write_text(json.dumps(dict(run_id='r',lease_path=str(lease),absolute_deadline_unix=10**12)))
            entry=dict(config_path=str(config),config_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),run_id='r',output_path=str(root/'output'))
            self.assertEqual(remote_control('poll',entry)['status'],'aborting')

    def test_recorded_child_anchors_cleanup_after_group_leader_disappears(self):
        leader=dict(pid=10,pgid=10,session=10,start_ticks=100)
        child=dict(pid=11,pgid=10,session=10,start_ticks=110)
        leader['verified_members']=[child]
        with tempfile.TemporaryDirectory() as temp, patch('tools.hierarchical_sharding.resource_guard.same_process',side_effect=lambda row:row['pid']==11) as verified,patch('tools.hierarchical_sharding.resource_guard.group_members',side_effect=[[child],[],[]]),patch('tools.hierarchical_sharding.resource_guard.os.killpg') as signal:
            stop_groups([leader],Path(temp)/'journal',grace_s=0)
            self.assertEqual(signal.call_args_list[0].args[0],10)
            self.assertTrue(any(call.args[0]['pid']==11 for call in verified.call_args_list))

    def test_unidentified_surviving_group_still_fails_closed(self):
        leader=dict(pid=10,pgid=10,session=10,start_ticks=100)
        with tempfile.TemporaryDirectory() as temp,patch('tools.hierarchical_sharding.resource_guard.same_process',return_value=False),patch('tools.hierarchical_sharding.resource_guard.group_members',return_value=[dict(pid=11)]),patch('tools.hierarchical_sharding.resource_guard.os.killpg') as signal:
            with self.assertRaises(RuntimeError):stop_groups([leader],Path(temp)/'journal',grace_s=0)
            signal.assert_not_called()

    def test_private_attempt_and_reservation_survive_preparation_failure(self):
        from .dynamic_attempt import PreparationAttempt
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'attempt.json'
            attempt=PreparationAttempt(path,'parent','split',Path(temp)/'private',{'left':'worker-a'})
            self.assertEqual(json.loads(path.read_text())['status'],'preparing')
            attempt.reserve('worker-a','replica','left',2,Path(temp)/'private')
            persisted=json.loads(path.read_text())
            self.assertEqual(persisted['remote_reservations'][0]['replica_id'],'replica')
            self.assertEqual(persisted['parent_sha256'],'parent')
            with self.assertRaises(FileExistsError):PreparationAttempt(path,'new','split','unused',{})
            attempt.prepared({'candidate':'hash-bound'})
            self.assertEqual(json.loads(path.read_text())['status'],'prepared')

    @unittest.skipUnless(__import__('sys').platform=='linux','actual /proc process identity needs Linux')
    def test_actual_child_survives_leader_exit_and_recorded_anchor_stops_it(self):
        import subprocess,sys,time,os
        from .resource_guard import process_identity,group_members
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);ready=root/'ready';release=root/'release'
            code="import subprocess,sys,time,pathlib; subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']); pathlib.Path(sys.argv[1]).write_text('ready');\nwhile not pathlib.Path(sys.argv[2]).exists():time.sleep(.01)"
            process=subprocess.Popen([sys.executable,'-c',code,str(ready),str(release)],start_new_session=True)
            identity=None
            try:
                until=time.monotonic()+3
                while not ready.exists() and time.monotonic()<until:time.sleep(.01)
                self.assertTrue(ready.exists())
                identity=process_identity(process.pid);members=group_members(identity)
                self.assertGreaterEqual(len(members),2)
                identity['verified_members']=members
                release.touch();process.wait(timeout=3)
                self.assertTrue(group_members(identity))
                stop_groups([identity],root/'cleanup',grace_s=.1)
                self.assertEqual(group_members(identity),[])
            finally:
                if identity and group_members(identity):
                    # The same recorded identities still bound cleanup on failure.
                    stop_groups([identity],root/'finally',grace_s=.1)
                if process.poll() is None:process.kill();process.wait()

    def test_failed_terminal_latches_while_other_poll_is_still_pending(self):
        import threading
        latch=threading.Event();observed=[]
        terminal=dict(status='failed',reconciled=True,guard_report={})
        def control(action,entry,timeout):
            if action=='launch':return dict(status='launched',identity={'pid':1},commands=1,lease_ttl_s=30)
            if action=='poll':
                if entry['host']=='b':
                    observed.append(latch.wait(.5))
                return dict(status='terminal',terminal=terminal)
            return {}
        with tempfile.TemporaryDirectory() as temp:
            report=run_batch(dict(hosts=[dict(host=h,output_path='/unused') for h in ('a','b')],control_timeout_s=1),Path(temp)/'batch',control,sleep=lambda _:None,on_failure=lambda reason:latch.set())
        self.assertEqual(observed,[True]);self.assertFalse(report['publication_allowed'])


    def test_racing_publisher_rechecks_failure_latched_during_final_poll(self):
        import threading
        from .dynamic_twohost_race import publication_health
        for mechanism in ('event','marker','stop'):
            with self.subTest(mechanism=mechanism), tempfile.TemporaryDirectory() as temp:
                stop=threading.Event();failure=threading.Event();marker=Path(temp)/'failure'
                def control(action,entry,timeout):
                    if mechanism=='event':failure.set()
                    elif mechanism=='marker':marker.touch()
                    else:stop.set()
                    return dict(status='running')
                with self.assertRaisesRegex(ValueError,'failure'):
                    publication_health([{'host':'last'}],stop,failure,str(marker),control)
