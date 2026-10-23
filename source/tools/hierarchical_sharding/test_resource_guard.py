"""Check owned scope accounting and process identity boundaries."""
import os
import sys
import json
import signal
import subprocess
import time
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from .resource_guard import inventory, violations, stop_groups, same_process, supervise, writer_executor


class GuardTests(unittest.TestCase):
    def test_hardlinks_count_once_and_overlapping_scopes_reject(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();(root/'a').write_bytes(b'abc');os.link(root/'a',root/'b')
            row=inventory([dict(path=str(root),role='prepared-and-checkpoints')],100)
            self.assertEqual(row['scopes'][0]['logical_bytes'],6);self.assertEqual(row['total_unique_inode_bytes'],3)
            (root/'child').mkdir()
            with self.assertRaisesRegex(ValueError,'overlap'):inventory([dict(path=str(root),role='a'),dict(path=str(root/'child'),role='b')],100)

    def test_sparse_file_allocated_bytes_and_hardlinks_are_separate(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();path=root/'sparse'
            with path.open('wb') as stream:stream.truncate(16*1024*1024)
            os.link(path,root/'same-inode')
            row=inventory([dict(path=str(root),role='checkpoint')],100)
            self.assertEqual(row['scopes'][0]['logical_bytes'],32*1024*1024)
            self.assertEqual(row['total_unique_inode_bytes'],16*1024*1024)
            self.assertEqual(row['total_unique_inode_allocated_bytes'],path.stat().st_blocks*512)
            self.assertEqual(row['scopes'][0]['unique_inode_allocated_bytes'],row['total_unique_inode_allocated_bytes'])

    def test_atomic_rename_between_walk_and_stat_does_not_abort_inventory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();(root/'gone').write_text('temporary')
            original=Path.lstat
            def racing(path,*args,**kwargs):
                if path.name=='gone':raise FileNotFoundError('renamed')
                return original(path,*args,**kwargs)
            with patch.object(Path,'lstat',racing):
                row=inventory([dict(path=str(root),role='staging')],100)
            self.assertEqual(row['total_unique_inode_bytes'],0)

    def test_unknown_symlink_and_inventory_limit_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();(root/'link').symlink_to('/tmp')
            with self.assertRaisesRegex(ValueError,'symlink'):inventory([dict(path=str(root),role='owned')],100)
            (root/'link').unlink();(root/'a').touch()
            with self.assertRaisesRegex(ValueError,'entry limit'):inventory([dict(path=str(root),role='owned')],0)

    def test_interruption_cleans_groups_before_executor_waits(self):
        events=[]
        class Executor:
            def __enter__(self):events.append('enter');return self
            def __exit__(self,*args):events.append('executor-waits')
        with patch('concurrent.futures.ThreadPoolExecutor',return_value=Executor()):
            with self.assertRaises(KeyboardInterrupt):
                with writer_executor(1,lambda error:events.append('stop-groups')):
                    raise KeyboardInterrupt('signal during monitor sleep')
        self.assertEqual(events,['enter','stop-groups','executor-waits'])

    def test_stop_margins_trigger_before_caps(self):
        limits=dict(scope_bytes_cap=100,scope_stop_margin_bytes=10,disk_free_floor_bytes=20,disk_stop_margin_bytes=5,rss_cap_bytes=100,rss_stop_margin_bytes=10)
        row=dict(total_unique_inode_bytes=90,filesystems={'x':dict(available_bytes=25)},aggregate_writer_rss_bytes=90)
        self.assertEqual(len(violations(row,limits)),3)
        row.update(total_unique_inode_bytes=89,aggregate_writer_rss_bytes=89);row['filesystems']['x']['available_bytes']=26
        self.assertEqual(violations(row,limits),[])

    def test_pid_reuse_never_signals_unverified_group(self):
        identity=dict(pid=10,pgid=10,session=10,start_ticks=20)
        with tempfile.TemporaryDirectory() as directory,patch('tools.hierarchical_sharding.resource_guard.process_identity',return_value=dict(identity,start_ticks=99)),patch('tools.hierarchical_sharding.resource_guard.group_members',return_value=[]),patch('os.killpg') as kill:
            self.assertFalse(same_process(identity));stop_groups([identity],Path(directory)/'journal');kill.assert_not_called()

    def test_unanchored_surviving_group_reports_unknown_without_killing(self):
        identity=dict(pid=10,pgid=10,session=10,start_ticks=20)
        with tempfile.TemporaryDirectory() as directory,patch('tools.hierarchical_sharding.resource_guard.same_process',return_value=False),patch('tools.hierarchical_sharding.resource_guard.group_members',return_value=[dict(pid=11)]),patch('os.killpg') as kill:
            with self.assertRaisesRegex(RuntimeError,'reconciled'):stop_groups([identity],Path(directory)/'journal')
            kill.assert_not_called()

class LinuxSupervisorTests(unittest.TestCase):
    @unittest.skipUnless(Path('/proc/self/stat').exists(),'Linux signal integration required')
    def test_sigterm_during_monitor_sleep_stops_writer_promptly(self):
        from .host_supervisor import writer_identities
        from .resource_guard import group_members
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();scope=root/'scope';scope.mkdir();output=root/'evidence'
            config=dict(abort_plan='verified-process-groups-no-detach',scopes=[dict(path=str(scope),role='owned')],commands=[[sys.executable,'-c','import time;time.sleep(30)']],concurrency=1,sample_interval_s=5,termination_grace_s=.1,timeout_s=40,limits=dict(scope_bytes_cap=1000000,scope_stop_margin_bytes=1000,disk_free_floor_bytes=1,disk_stop_margin_bytes=1,rss_cap_bytes=1000000000,rss_stop_margin_bytes=1000000))
            path=root/'config';path.write_text(json.dumps(config))
            with (root/'stdout').open('wb') as out,(root/'stderr').open('wb') as err:
                process=subprocess.Popen([sys.executable,'-m','tools.hierarchical_sharding.resource_guard','--config',str(path),'--output',str(output)],stdout=out,stderr=err)
                try:
                    deadline=time.monotonic()+10
                    while time.monotonic()<deadline:
                        journal_path=output/'journal.jsonl'
                        text=journal_path.read_text() if journal_path.exists() else ''
                        if 'writer-start' in text and text.count('resource-sample')>=2:break
                        time.sleep(.02)
                    else:self.fail('guard did not reach monitor sleep')
                    began=time.monotonic();process.send_signal(signal.SIGTERM);process.wait(timeout=3)
                    latency=time.monotonic()-began
                    self.assertLess(latency,3)
                    self.assertFalse(any(group_members(i) for i in writer_identities(output/'journal.jsonl')))
                    print('guard SIGTERM cleanup seconds:',latency)
                finally:
                    if process.poll() is None:
                        stop_groups(writer_identities(output/'journal.jsonl'),root/'cleanup-journal')
                        process.kill();process.wait(timeout=3)

    @unittest.skipUnless(Path('/proc/self/stat').exists(),'Linux process identity required')
    def test_scope_abort_stops_owned_parent_and_term_resistant_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory).resolve();scope=root/'owned';scope.mkdir()
            script="import subprocess,sys,time,pathlib; child=subprocess.Popen([sys.executable,'-c','import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(30)']); time.sleep(.2); pathlib.Path(sys.argv[1]).write_bytes(b'x'*950); time.sleep(30)"
            config=dict(abort_plan='verified-process-groups-no-detach',scopes=[dict(path=str(scope),role='cloned-checkpoint')],commands=[[sys.executable,'-c',script,str(scope/'grown')]],concurrency=1,sample_interval_s=.05,termination_grace_s=.1,timeout_s=5,limits=dict(scope_bytes_cap=1000,scope_stop_margin_bytes=100,disk_free_floor_bytes=1,disk_stop_margin_bytes=1,rss_cap_bytes=1000000000,rss_stop_margin_bytes=1000000))
            report=supervise(config,root/'evidence')
            self.assertEqual(report['status'],'aborted-or-failed')
            self.assertTrue(any('scope byte' in reason for reason in report['reasons']))
            self.assertEqual((scope/'grown').stat().st_size,950)
            text=(root/'evidence/journal.jsonl').read_text();self.assertIn('kill-group',text);self.assertIn('abort-reconciliation',text)

if __name__=='__main__':unittest.main()
