"""Control-plane failures never become successful distributed publication."""
from pathlib import Path
import tempfile
import unittest
import hashlib
import json
import os
import sys
import time
from .guarded_host_batch import accepted_terminal,run_batch,remote_control


def terminal(status='passed',reconciled=True):
    return dict(status=status,reconciled=reconciled,guard_report=dict(status=status,outcomes=[dict(index=0,status='ok',returncode=0)]))


class BatchTests(unittest.TestCase):
    def config(self):
        return dict(hosts=[dict(host=host,output_path='/new/'+host,run_id=host,config_path='/config',config_sha256='hash') for host in ('a','b')],poll_interval_s=1,heartbeat_interval_s=2,control_timeout_s=1,timeout_s=5)

    def test_success_requires_every_reconciled_local_command_outcome(self):
        def control(action,entry,timeout):
            if action=='launch':return dict(status='launched',identity=dict(pid=1),commands=1,lease_ttl_s=60)
            if action=='poll':return dict(status='terminal',terminal=terminal())
            return dict(status='ok')
        with tempfile.TemporaryDirectory() as directory:
            report=run_batch(self.config(),Path(directory)/'run',control=control)
            self.assertTrue(report['complete']);self.assertTrue(report['publication_allowed']);self.assertEqual(set(report['hosts']),{'a','b'})
        value=terminal();value['guard_report']['outcomes'][0]['returncode']=1
        self.assertFalse(accepted_terminal(value,1));self.assertFalse(accepted_terminal(terminal(reconciled=False),1))

    def test_peer_failure_aborts_every_host_and_keeps_partial_outcomes(self):
        calls=[]
        def control(action,entry,timeout):
            calls.append((action,entry['host']))
            if action=='launch':return dict(status='launched',identity=dict(pid=1),commands=1,lease_ttl_s=60)
            if action=='poll':return dict(status='terminal',terminal=terminal('failed' if entry['host']=='a' else 'passed'))
            return dict(status='ok')
        with tempfile.TemporaryDirectory() as directory:
            report=run_batch(self.config(),Path(directory)/'run',control=control)
            self.assertFalse(report['complete']);self.assertIn(('abort','a'),calls);self.assertIn(('abort','b'),calls);self.assertEqual(len(report['hosts']),2)

    def test_lost_control_is_unreconciled_and_never_retried(self):
        now=[0];launches=[]
        def control(action,entry,timeout):
            if action=='launch':launches.append(entry['host']);return dict(status='launched',identity=dict(pid=1),commands=1,lease_ttl_s=60)
            if entry['host']=='a':raise TimeoutError('unreachable')
            if action=='poll':return dict(status='terminal',terminal=terminal())
            return dict(status='ok')
        def sleep(seconds):now[0]+=seconds
        with tempfile.TemporaryDirectory() as directory:
            report=run_batch(self.config(),Path(directory)/'run',control=control,clock=lambda:now[0],sleep=sleep)
            self.assertFalse(report['publication_allowed']);self.assertEqual(report['missing_terminal_hosts'],['a']);self.assertEqual(sorted(launches),['a','b']);self.assertFalse(report['retry_allowed'])

    def test_heartbeat_failure_aborts_other_worker(self):
        now=[0];calls=[]
        def control(action,entry,timeout):
            calls.append((action,entry['host']))
            if action=='launch':return dict(status='launched',identity=dict(pid=1),commands=1,lease_ttl_s=60)
            if action=='renew':raise TimeoutError('heartbeat failed')
            if action=='poll':return dict(status='terminal',terminal=terminal('failed')) if any(a=='abort' for a,h in calls) else dict(status='running')
            return dict(status='ok')
        def sleep(seconds):now[0]+=seconds
        with tempfile.TemporaryDirectory() as directory:
            report=run_batch(self.config(),Path(directory)/'run',control=control,clock=lambda:now[0],sleep=sleep)
            self.assertFalse(report['complete']);self.assertTrue(any(a=='renew' for a,h in calls));self.assertTrue(any(a=='abort' for a,h in calls))

class LinuxBatchTests(unittest.TestCase):
    @unittest.skipUnless(Path('/proc/self/stat').exists(),'Linux local-supervisor integration required')
    def test_actual_peer_failure_stops_other_hosts_local_writer(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();entries=[]
            for index in range(2):
                scope=root/('scope-'+str(index));scope.mkdir();lease=root/('lease-'+str(index))
                command=[sys.executable,'-c','import time;time.sleep(.3);raise SystemExit(3)' if index==0 else 'import time;time.sleep(30)']
                guard=dict(commands=[command],concurrency=1,scopes=[dict(path=str(scope),role='owned-checkpoint')],abort_plan='verified-process-groups-no-detach',sample_interval_s=.05,termination_grace_s=.1,limits=dict(scope_bytes_cap=1000000,scope_stop_margin_bytes=1000,disk_free_floor_bytes=1,disk_stop_margin_bytes=1,rss_cap_bytes=1000000000,rss_stop_margin_bytes=1000000))
                config=dict(run_id='test-'+str(index),cpu_affinity=str(min(os.sched_getaffinity(0))),lease_path=str(lease),lease_ttl_s=2,absolute_deadline_unix=time.time()+15,guard=guard)
                path=root/('config-'+str(index));path.write_text(json.dumps(config))
                entries.append(dict(host='local-'+str(index),config_path=str(path),config_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),run_id=config['run_id'],output_path=str(root/('host-'+str(index))),source_root=str(Path(__file__).resolve().parents[2])))
            config=dict(hosts=entries,poll_interval_s=.05,heartbeat_interval_s=.2,control_timeout_s=.2,timeout_s=10)
            report=run_batch(config,root/'driver',control=lambda action,entry,timeout:remote_control(action,entry))
            self.assertFalse(report['complete']);self.assertFalse(report['missing_terminal_hosts'])
            self.assertTrue(all(t['reconciled'] for t in report['hosts'].values()))
            self.assertEqual(report['hosts']['local-1']['status'],'aborted-or-failed')

if __name__=='__main__':unittest.main()
