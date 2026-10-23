"""Check bounded driver-loss cleanup without touching any benchmark store."""
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from .host_supervisor import lease_reason,renew_lease,validate_config,writer_identities,supervise_host


class HostSupervisorTests(unittest.TestCase):
    def config(self):
        return dict(run_id='test-run',cpu_affinity='0',lease_ttl_s=2,absolute_deadline_unix=time.time()+30,guard=dict(commands=[[sys.executable,'-c','pass']],concurrency=1,abort_plan='verified-process-groups-no-detach'))

    def test_lease_binds_run_and_expires_on_worker_clock(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'lease';renew_lease(path,'a');value=json.loads(path.read_text())
            self.assertIsNone(lease_reason(path,'a',60,value['renewed_unix']+59))
            self.assertIsNotNone(lease_reason(path,'a',60,value['renewed_unix']+61))
            self.assertIsNotNone(lease_reason(path,'b',60,value['renewed_unix']))
            with self.assertRaisesRegex(ValueError,'another run'):renew_lease(path,'b')
            self.assertIsNotNone(lease_reason(path,'a',60,value['renewed_unix']-1))

    def test_remote_commands_and_missing_deadline_reject(self):
        config=self.config();config['guard']['commands']=[['ssh','worker','writer']]
        with self.assertRaisesRegex(ValueError,'local'):validate_config(config,time.time())
        config=self.config();config['guard']['commands']=['shell command']
        with self.assertRaisesRegex(ValueError,'argv'):validate_config(config,time.time())
        config=self.config();config['absolute_deadline_unix']=time.time()-1
        with self.assertRaisesRegex(ValueError,'deadline'):validate_config(config,time.time())

    def test_partial_final_journal_keeps_prior_writer_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'journal';identity=dict(pid=1,start_ticks=2)
            path.write_text(json.dumps(dict(phase='writer-start',identity=identity))+'\n{"partial":')
            self.assertEqual(writer_identities(path),[identity])

    @unittest.skipUnless(Path('/proc/self/stat').exists(),'Linux process-group integration required')
    def test_expired_driver_lease_stops_local_writer_and_preserves_terminal(self):
        original=os.sched_getaffinity(0)
        try:
            with tempfile.TemporaryDirectory() as directory:
                root=Path(directory).resolve();scope=root/'owned';scope.mkdir();lease=root/'lease';renew_lease(lease,'test-run')
                config=self.config();config.update(cpu_affinity=str(min(original)),lease_path=str(lease))
                config['guard'].update(commands=[[sys.executable,'-c','import time;time.sleep(30)']],scopes=[dict(path=str(scope),role='cloned-index')],sample_interval_s=.05,termination_grace_s=.1,limits=dict(scope_bytes_cap=1000000,scope_stop_margin_bytes=1000,disk_free_floor_bytes=1,disk_stop_margin_bytes=1,rss_cap_bytes=1000000000,rss_stop_margin_bytes=1000000))
                terminal=supervise_host(config,root/'evidence')
                self.assertEqual(terminal['status'],'aborted-or-failed');self.assertTrue(terminal['reconciled']);self.assertIn('expired',terminal['reason']);self.assertTrue(terminal['known_writer_groups'])
                identity=json.loads((root/'evidence/identity.json').read_text());self.assertEqual(identity['cpu_affinity'],[min(original)])
        finally:os.sched_setaffinity(0,original)

if __name__=='__main__':unittest.main()
