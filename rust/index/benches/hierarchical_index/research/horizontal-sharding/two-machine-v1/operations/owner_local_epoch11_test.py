"""Mock-only epoch11 selection, PID binding, bounds and collector cleanup tests."""
import argparse
import copy
import importlib.util
from pathlib import Path
import signal
import subprocess
import unittest
from unittest.mock import Mock, patch

HERE=Path(__file__).resolve().parent

def load(name,file):
    location=importlib.util.spec_from_file_location(name,HERE/file)
    module=importlib.util.module_from_spec(location);location.loader.exec_module(module);return module

g=load("epoch11_spec","make-owner-local-spec-epoch11.py")
p=load("epoch11_profile","remote-profile-epoch11.py")

def arguments(mode="run"):
    return argparse.Namespace(arm="100k-profile",mode=mode,window_id="epoch11-test",
        window_deadline=1900,job_deadline=1590,worker_ip="10.0.0.1",peer_ip="10.0.0.2",
        base=g.BASE,corpus_descriptor=g.DESCRIPTOR,input_file=g.INPUT)


class ProfileTests(unittest.TestCase):
    def test_exact_corrected_runtime_and_only_fixed_profile_arm(self):
        value=g.generate(arguments(),1000);peer=g.generate(arguments("serve"),1000)
        self.assertEqual(value["binary"],p.BINARY)
        self.assertEqual(value["binary_sha256"],p.BINARY_SHA)
        self.assertEqual(value["deadline_unix"],1590)
        self.assertEqual(value["workload_timeout_seconds"],600)
        self.assertEqual(value["args"]["--run"],peer["args"]["--run"])
        p.selected_spec(value,"a"*64,1000)
        for field,new in (("arm","3m-distributed"),("arm","100k-after"),("window_id","epoch9-closed"),("job_deadline",1591)):
            changed=copy.copy(arguments());setattr(changed,field,new)
            with self.assertRaises(ValueError):g.generate(changed,1000)
        for mutate in (lambda s:s.update(binary_sha256="a"*64),lambda s:s["args"].update({"--rows":3000000}),
                       lambda s:s["args"].update({"--input-descriptor":"/other/100k.json"}),lambda s:s.update(mode="serve")):
            changed=copy.deepcopy(value);mutate(changed)
            with self.assertRaises(ValueError):p.selected_spec(changed,"a"*64,1000)

    def test_pid_boot_start_ticks_executable_marker_and_spec_are_bound(self):
        spec=g.generate(arguments(),1000);digest="a"*64
        receipt=dict(pid=410,pgid=410,start_ticks=123,boot_id="boot",marker="100k-profile-owner1:"+digest,
                     job_id="100k-profile-owner1",instance_id=g.WORKER,spec_sha256=digest,deadline_unix=1590)
        with patch.object(p,"start_ticks",return_value=123),patch.object(p.os,"getpgid",return_value=410),\
             patch.object(Path,"resolve",return_value=Path(p.BINARY)),patch.object(Path,"read_text",return_value="boot"),\
             patch.object(Path,"read_bytes",return_value=("SPANN_OWNED_JOB="+receipt["marker"]).encode()+b"\0"):
            self.assertEqual(p.verify_process(receipt,spec,digest),410)
            for key,value in (("start_ticks",124),("boot_id","other"),("spec_sha256","b"*64),("pgid",411),("pid",1),("marker","other")):
                changed=receipt.copy();changed[key]=value
                with self.assertRaises(ValueError):p.verify_process(changed,spec,digest)
            with patch.object(Path,"resolve",return_value=Path("/unrelated/program")):
                with self.assertRaises(ValueError):p.verify_process(receipt,spec,digest)
            with patch.object(Path,"read_bytes",return_value=b"OTHER=value\0"):
                with self.assertRaises(ValueError):p.verify_process(receipt,spec,digest)

    def test_recording_command_is_pid_scoped_and_hard_bounded(self):
        command=p.recording_command(410,Path("/fresh/perf.data"))
        self.assertEqual(command[:4],[p.TIMEOUT,"--signal=INT","--kill-after=1","9"])
        self.assertEqual(command[command.index("--pid")+1],"410")
        self.assertEqual(command[command.index("--freq")+1],"99")
        self.assertEqual(command[command.index("--event")+1],"cpu-clock:u")
        self.assertEqual(command[command.index("--clockid")+1],"CLOCK_REALTIME")
        self.assertNotIn("--all-cpus",command);self.assertNotIn("-a",command)
        self.assertEqual(p.MAX_BYTES,256*1024**2)
        with patch.object(p.resource,"setrlimit") as limit:p.file_cap()
        limit.assert_called_once_with(p.resource.RLIMIT_FSIZE,(p.MAX_BYTES,p.MAX_BYTES))
        events=[{"event":"balance_started","rows":3000000},{"event":"balance_started","rows":100000}]
        self.assertEqual(p.phase(events,"balance_started"),events[1])
        self.assertIsNone(p.phase(events,"balance_complete"))
        with patch.object(Path,"exists",return_value=True),patch.object(Path,"read_text",return_value='{"event":"balance_started","rows":100000}\n{"event":'):
            self.assertEqual(p.observations(Path("/events")),[events[1]])

    def test_cleanup_signals_only_marked_collector_group_and_escalates_with_bound(self):
        child=Mock(pid=510,profile_marker="collector-spec")
        with patch.object(p,"collector_members",side_effect=[[510],[],[],[]]),patch.object(p.os,"killpg") as kill:
            p.stop_collector(child)
            kill.assert_called_once_with(510,signal.SIGINT)
            child.wait.assert_called_with(timeout=1)
        with patch.object(p,"collector_members",side_effect=[[510],[510],[510],[]]),\
             patch.object(p.time,"monotonic",side_effect=[100,102]),patch.object(p.os,"killpg") as kill:
            p.stop_collector(child)
            self.assertEqual([call.args for call in kill.call_args_list],[(510,signal.SIGINT),(510,signal.SIGKILL)])
        with patch.object(p,"collector_members",side_effect=ValueError("unowned member")),patch.object(p.os,"killpg") as kill:
            with self.assertRaises(ValueError):p.stop_collector(child)
            kill.assert_not_called()
        child.wait.side_effect=subprocess.TimeoutExpired("collector",1)
        with patch.object(p,"collector_members",return_value=[]):
            with self.assertRaises(RuntimeError):p.stop_collector(child)

    def test_preflight_fails_closed_on_missing_options_or_sampling_permissions(self):
        version=Mock(stdout="perf version 7.0.14\n")
        help_result=Mock(stdout="--pid --freq --event --clockid --call-graph --output --no-inherit --no-buildid-cache",stderr="",returncode=129)
        with patch.object(Path,"is_file",return_value=True),patch.object(p.os,"access",return_value=True),\
             patch.object(p.subprocess,"run",side_effect=[version,help_result]),patch.object(Path,"read_text",return_value="4"),\
             patch.object(p.os,"geteuid",return_value=1000):
            with self.assertRaises(PermissionError):p.preflight()
        help_result.stdout="--pid"
        with patch.object(Path,"is_file",return_value=True),patch.object(p.os,"access",return_value=True),\
             patch.object(p.subprocess,"run",side_effect=[version,help_result]):
            with self.assertRaises(ValueError):p.preflight()


class PrivilegedCollectorTests(unittest.TestCase):
    def test_exact_noninteractive_isolated_command_hashes_source_before_execution(self):
        import hashlib
        import sys
        runner=load("epoch11_runner","run-one-arm-epoch11.py")
        source=b"raise RuntimeError('verified collector body')"
        digest=hashlib.sha256(source).hexdigest()
        command=runner.privileged_collector_command("/spec.json","a"*64,"/evidence","/output",digest)
        self.assertEqual(command[:7],["sudo","-n","/usr/bin/timeout","--signal=TERM","--kill-after=2","118","/usr/bin/python3"])
        self.assertEqual(command[7:9],["-I","-c"])
        self.assertNotIn("/bin/sh",command)
        bootstrap=command[9]
        path="/home/ubuntu/spann-two-machine-v1/epoch11/remote-profile-epoch11.py"
        with patch.object(sys,"argv",["-c",path,"0"*64]),patch.object(Path,"read_bytes",return_value=source):
            with self.assertRaises(AssertionError):exec(bootstrap,{})
        with patch.object(sys,"argv",["-c",path,digest]),patch.object(Path,"read_bytes",return_value=source):
            with self.assertRaisesRegex(RuntimeError,"verified collector body"):exec(bootstrap,{})
        with patch.object(sys,"argv",["-c","/unreviewed/script.py",digest]),patch.object(Path,"read_bytes",return_value=source):
            with self.assertRaises(AssertionError):exec(bootstrap,{})

    def test_root_sampler_does_not_change_paranoid_four(self):
        version=Mock(stdout="perf version 7.0.14\n")
        help_result=Mock(stdout="--pid --freq --event --clockid --call-graph --output --no-inherit --no-buildid-cache",stderr="",returncode=129)
        with patch.object(Path,"is_file",return_value=True),patch.object(p.os,"access",return_value=True),\
             patch.object(p.subprocess,"run",side_effect=[version,help_result]),patch.object(Path,"read_text",return_value="4"),\
             patch.object(p.os,"geteuid",return_value=0),patch.object(Path,"write_text") as write:
            receipt=p.preflight()
            self.assertEqual(receipt["perf_event_paranoid"],"4")
            self.assertEqual(receipt["effective_uid"],0)
            self.assertFalse(receipt["permissions_changed"])
            write.assert_not_called()


class LateReportTests(unittest.TestCase):
    def test_final_report_collection_requires_readable_report_and_excludes_raw_data(self):
        import json
        runner=load("epoch11_final_report","run-one-arm-epoch11.py")
        root="/home/ubuntu/spann-two-machine-v1/epoch11/100k-profile-collector"
        final={root+"/perf-report.txt":"final complete report",root+"/result.json":'{"status":"completed"}'}
        with patch.object(runner,"ssh",return_value=json.dumps(final)) as ssh:
            self.assertEqual(runner.collect_final_profile_files("192.0.2.1",root),final)
            self.assertNotIn("perf.data",ssh.call_args.args[1])
        with patch.object(runner,"ssh",return_value="{}"):
            with self.assertRaisesRegex(RuntimeError,"final report"):runner.collect_final_profile_files("192.0.2.1",root)


if __name__=="__main__":unittest.main()
