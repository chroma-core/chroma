"""Require exact local argv and every host's verification before publication."""
import hashlib
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from .build import command
from .guarded_compaction_backend import validate_jobs,inspect_parallel,run_guarded_jobs,covered,validate_remote_paths,summarize_log


class GuardedCompactionTests(unittest.TestCase):
    def fixture(self,root):
        job=dict(host='worker',binary='/measured',vectors='/owned/delta.f32',id_map='/owned/map.u64le',checkpoint_dir='/owned/index',shard_id='a',source_id='s',dimension=1,row_count=2,global_id_start=1,id_stride=1,generation=2,metric='l2',resume=True,cpu_affinity='0',threads=1,runtime_threads=2)
        config=dict(run_id='run',cpu_affinity='0',lease_path='/owned/lease',guard=dict(commands=[command(dict(job,host='local'))],concurrency=1,scopes=[dict(path='/owned',role='run')]))
        path=Path(root)/'host.json';path.write_text(json.dumps(config));entry=dict(host='worker',local_config_path=str(path),config_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),run_id='run',output_path='/owned/guard',job_shard_ids=['a'])
        return [job],dict(hosts=[entry]),config,path

    def test_traversal_and_symlink_ancestry_are_rejected(self):
        self.assertFalse(covered('/owned/../outside',[dict(path='/owned')]))
        self.assertFalse(covered('/owned/file',[dict(path='/owned/../outside')]))
        with tempfile.TemporaryDirectory() as d:
            root=Path(d).resolve();(root/'real').mkdir();(root/'alias').symlink_to(root/'real',target_is_directory=True)
            validate_remote_paths([str(root/'real'/'new'/'index')])
            with self.assertRaisesRegex(ValueError,'symlink'):validate_remote_paths([str(root/'alias'/'new'/'index')])
            with self.assertRaisesRegex(ValueError,'unsafe'):validate_remote_paths([str(root/'real'/'..'/'outside')])

    def test_remote_path_admission_failure_precedes_output_or_writer_launch(self):
        with tempfile.TemporaryDirectory() as d:
            jobs,batch,_,_=self.fixture(d);output=Path(d)/'build'
            with patch('tools.hierarchical_sharding.guarded_compaction_backend.admit_remote_paths',side_effect=ValueError('symlink')) as admission:
                with self.assertRaisesRegex(ValueError,'symlink'):run_guarded_jobs(jobs,batch,output,'0',1)
                admission.assert_called_once()
            self.assertFalse(output.exists())

    def test_full_log_streaming_keeps_hash_counts_and_middle_warning(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'raw';body=b'a'*(1024*1024-8)+b'NavigationMode is not full precision f32\n'+b'b'*(2*1024*1024)+b'without full convergence\nlast'
            path.write_bytes(body);result=summarize_log(path,32)
            self.assertEqual(result['sha256'],hashlib.sha256(body).hexdigest())
            self.assertEqual(result['bytes'],len(body));self.assertEqual(result['lines'],3)
            self.assertEqual(result['classification_occurrences']['navigation_precision_warning'],1)
            self.assertEqual(result['classification_occurrences']['balance_nonconvergence'],1)
            self.assertLess(len(json.dumps(result)),2048)
            self.assertEqual(path.read_bytes(),body)
    def test_missing_and_symlink_raw_logs_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);path=root/'missing'
            with self.assertRaises(ValueError):summarize_log(path)
            (root/'raw').write_bytes(b'complete');path.symlink_to(root/'raw')
            with self.assertRaises(ValueError):summarize_log(path)
    def test_concurrently_changed_raw_log_fails_closed(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'raw';path.write_bytes(b'initial')
            from contextlib import contextmanager
            original=Path.open
            @contextmanager
            def changing_open(value,*args,**kwargs):
                with original(value,*args,**kwargs) as stream:yield stream
                with original(value,'ab') as stream:stream.write(b'changed')
            with patch.object(Path,'open',changing_open):
                with self.assertRaisesRegex(ValueError,'changes'):summarize_log(path)

    def test_exact_local_argv_allocation_and_scopes(self):
        with tempfile.TemporaryDirectory() as directory:
            jobs,batch,config,path=self.fixture(directory);validate_jobs(jobs,batch,'0',1)
            for field in ('argv','scope','mask'):
                original=json.loads(path.read_text())
                if field=='argv':original['guard']['commands']=[['ssh','elsewhere']]
                elif field=='scope':original['guard']['scopes']=[dict(path='/other',role='wrong')]
                else:original['cpu_affinity']='1'
                path.write_text(json.dumps(original));batch['hosts'][0]['config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
                with self.subTest(field=field),self.assertRaises(ValueError):validate_jobs(jobs,batch,'0',1)
                path.write_text(json.dumps(config))

    def test_inner_taskset_cannot_escape_supervisor_cpu_mask(self):
        with tempfile.TemporaryDirectory() as directory:
            jobs,batch,config,path=self.fixture(directory);jobs[0]['cpu_affinity']='1'
            config['guard']['commands']=[command(dict(jobs[0],host='local'))]
            path.write_text(json.dumps(config));batch['hosts'][0]['config_sha256']=hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaisesRegex(ValueError,'writer argv CPU'):validate_jobs(jobs,batch,'0',1)

    def test_parallel_verification_bounds_hosts_and_failure_never_reaches_cas(self):
        lock=threading.Lock();active=[0];peak=[0];published=[]
        entries={str(i):dict(host='h'+str(i),subtree_id=str(i)) for i in range(5)}
        def inspect(host,rows):
            with lock:active[0]+=1;peak[0]=max(peak[0],active[0])
            time.sleep(.02)
            with lock:active[0]-=1
            if host=='h3':raise ValueError('bad committed map')
            return rows
        def publish_after_verify():
            result=inspect_parallel(entries,inspect,lambda *args:None,'unused',2)
            published.append(result)
        with self.assertRaisesRegex(RuntimeError,'publication blocked'):publish_after_verify()
        self.assertEqual(peak[0],2);self.assertFalse(published)

    def test_guard_failure_still_preserves_owner_logs_and_blocks_success(self):
        with tempfile.TemporaryDirectory() as directory:
            jobs,batch,_,_=self.fixture(directory)
            result=dict(complete=False,hosts=dict(worker=dict(guard_report=dict(outcomes=[dict(index=0,status='failed',returncode=3)]))))
            def collect(entry):return dict(journal='preserved\n',logs=[dict(index=0,shard_id='a',stdout='partial',stderr='failure')],events=[dict(phase='writer-start',index=0,time_unix=10),dict(phase='writer-finish',index=0,time_unix=11)])
            report=run_guarded_jobs(jobs,batch,Path(directory)/'build','0',1,runner=lambda *args:result,collector=collect)
            self.assertFalse(report['complete']);self.assertEqual(report['host_writer_spans']['worker']['writer_span_s'],1)
            self.assertEqual((Path(directory)/'build/0.stdout').read_text(),'partial')
            self.assertEqual(report['jobs'][0]['returncode'],3)

    def test_aborted_guard_without_final_report_still_copies_partial_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            jobs,batch,_,_=self.fixture(directory)
            result=dict(complete=False,hosts=dict(worker=dict(guard_report=None)))
            def collect(entry):return dict(journal='partial\n',logs=[dict(index=0,shard_id='a',stdout='partial output',stderr='signal')],events=[dict(phase='writer-start',index=0,time_unix=10)])
            report=run_guarded_jobs(jobs,batch,Path(directory)/'build','0',1,runner=lambda *args:result,collector=collect)
            self.assertFalse(report['complete']);self.assertEqual(report['jobs'][0]['status'],'unknown')
            self.assertEqual((Path(directory)/'build/0.stderr').read_text(),'signal')

if __name__=='__main__':unittest.main()
