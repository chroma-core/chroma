"""Pure/mock profile-gate protocol and fixed100K pair tests; no real profiler starts."""
import argparse,hashlib,importlib.util,json,time,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch

HERE=Path(__file__).resolve().parent

def load(name,file):
 location=importlib.util.spec_from_file_location(name,HERE/file);module=importlib.util.module_from_spec(location);location.loader.exec_module(module);return module

g=load("epoch12_spec","make-owner-local-spec-epoch12b.py")
p=load("epoch12_profiler","remote-profile-epoch12b.py")
j=load("epoch12_job","remote-job-epoch12b.py")

def arguments(arm="100k-distributed",mode="run"):
 return argparse.Namespace(arm=arm,mode=mode,window_id="epoch12-test",window_deadline=1900,job_deadline=1590,
  worker_ip="10.0.0.1",peer_ip="10.0.0.2",base=g.BASE,corpus_descriptor=g.DESCRIPTOR,input_file=g.INPUT)

def generate(arm="100k-distributed",mode="run"):
 with patch.object(g,"artifact_bindings",return_value=({"two_machine_spann":"a"*64,"two_machine_reader":"b"*64,"reference":"c"*64},"d"*64)):
  return g.generate(arguments(arm,mode),1000)


class GateTests(unittest.TestCase):
 def test_fixed_matched_pair_and_only_owner1_gets_exact_gate(self):
  for arm in ("100k-distributed","100k-reference"):
   value=generate(arm);p.selected_spec(value,"a"*64,1000);j.validate(value,1000)
   self.assertEqual(value["args"]["--rows"],100000);self.assertEqual(value["args"]["--workers"],32)
   self.assertEqual(value["deadline_unix"],1590);self.assertEqual(value["workload_timeout_seconds"],600)
   flag="--output-dir" if arm=="100k-reference" else "--output"
   self.assertEqual(value["profile_gate"],value["args"][flag]+"-profile-gate")
   for gate in ("/tmp/unbound",value["args"][flag],value["profile_gate"]+"/../other"):
    changed=dict(value,profile_gate=gate)
    with self.assertRaises(ValueError):j.validate(changed,1000)
  self.assertNotIn("profile_gate",generate(mode="serve"))
  with patch.dict(j.os.environ,{"BENCH_PROFILE_GATE":"/stale/inherited"}):
   self.assertNotIn("BENCH_PROFILE_GATE",j.workload_environment(generate(mode="serve"),"marker"))
   self.assertEqual(j.workload_environment(generate(),"marker")["BENCH_PROFILE_GATE"],generate()["profile_gate"])
  with self.assertRaises(ValueError):generate("100k-reference","serve")
  for arm in ("3m-distributed","100k-two50k-distributed"):
   with self.assertRaises(ValueError):generate(arm)

 def test_control_ack_and_initial_disabled_bound(self):
  command=p.recording_command(410,Path("/out/perf.data"),Path("/out/control"),Path("/out/ack"))
  self.assertEqual(command[:4],[p.TIMEOUT,"--signal=INT","--kill-after=1","119"])
  limited=p.recording_command(410,Path("/out/perf.data"),Path("/out/control"),Path("/out/ack"),10)
  self.assertEqual(limited[3],"9")
  with self.assertRaises(ValueError):p.recording_command(410,Path("/out/p"),Path("/out/c"),Path("/out/a"),121)
  self.assertIn("--delay=-1",command);self.assertIn("--control=fifo:/out/control,/out/ack",command)
  self.assertEqual(command[command.index("--pid")+1],"410");self.assertNotIn("-a",command)
  child=Mock();child.poll.return_value=None
  with patch.object(p.os,"write") as write,patch.object(p.os,"read",side_effect=[BlockingIOError(),b"ack\n",b"\x00"]),patch.object(p.time,"sleep"):
   receipt=p.control_ack(1,2,"enable",child,time.monotonic()+1)
   self.assertEqual(receipt["ack"],"ack\n\x00");write.assert_called_once_with(1,b"enable\n")
  with patch.object(p.os,"write"),patch.object(p.os,"read",return_value=b"wrong\n\x00"):
   with self.assertRaises(ValueError):p.control_ack(1,2,"enable",child,time.monotonic()+1)
  with patch.object(p.os,"write"):
   with self.assertRaises(TimeoutError):p.control_ack(1,2,"enable",child,time.monotonic()-1)
  child.poll.return_value=1
  with patch.object(p.os,"write"):
   with self.assertRaises(RuntimeError):p.control_ack(1,2,"enable",child,time.monotonic()+1)

 def test_gate_rejects_wrong_pid_rows_or_phase(self):
  valid=dict(format_version=1,phase_id="balance-0",rows=100000,pid=410)
  self.assertEqual(p.gate_binding(valid,410),valid)
  for key,value in (("pid",411),("rows",3000000),("phase_id","balance-1"),("format_version",2)):
   bad=dict(valid);bad[key]=value
   with self.assertRaises(ValueError):p.gate_binding(bad,410)

 def run_mocked_collection(self,early_exit=False):
  with tempfile.TemporaryDirectory() as tmp:
   base=Path(tmp);arm="100k-distributed";job=arm+"-owner1";evidence=base/(job+"-process");evidence.mkdir()
   gate=base/(job+"-profile-gate");gate.mkdir();output=base/(arm+"-collector")
   binary=base/"binary";binary.write_bytes(b"fixed instrumented test binary")
   binding=dict(format_version=1,phase_id="balance-0",rows=100000,pid=410)
   (gate/"ready.json").write_text(json.dumps(binding))
   receipt=dict(pid=410,pgid=410,start_ticks=123,boot_id="boot",marker="marker",deadline_unix=int(time.time()+590))
   (evidence/"running.json").write_text(json.dumps(receipt))
   spec=dict(binary=str(binary),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),job_id=job,
    deadline_unix=int(time.time()+590),window_deadline_unix=int(time.time()+900),profile_gate=str(gate),
    provenance=dict(arm=arm,collector_script_sha256=hashlib.sha256(Path(p.__file__).read_bytes()).hexdigest()))
   spec_file=base/"spec.json";spec_file.write_text(json.dumps(spec));digest=hashlib.sha256(spec_file.read_bytes()).hexdigest()
   args=argparse.Namespace(spec=spec_file,spec_sha256=digest,evidence=evidence,output=output)
   order=[];original_write=p.write;child=Mock(pid=510,returncode=0)
   def write(path,value):order.append("write:"+path.name);return original_write(path,value)
   def popen(*args,**kwargs):(output/"perf.data").write_bytes(b"mock samples");return child
   def poll():
    if early_exit:return 1
    if not (gate/"done.json").exists():
     self.assertTrue((gate/"go.json").exists());write(gate/"done.json",dict(binding,status="completed",elapsed_seconds=.1))
    return None
   child.poll.side_effect=poll
   def ack(control_fd,ack_fd,command,child,deadline):order.append("ack:"+command);return dict(command=command,ack="ack\n",observed=p.stamp())
   def report(*args,**kwargs):order.append("report");kwargs["stdout"].write(b"complete report\n");return Mock(returncode=0)
   with patch.object(p,"BASE",str(base)),patch.object(p,"selected_spec"),patch.object(p,"preflight",return_value={}),\
        patch.object(p,"verify_process",return_value=410),patch.object(p,"write",side_effect=write),\
        patch.object(p.subprocess,"Popen",side_effect=popen),patch.object(p.subprocess,"run",side_effect=report),\
        patch.object(p,"control_ack",side_effect=ack),patch.object(p,"stop_collector",side_effect=lambda child:order.append("stop")),patch.object(p.time,"sleep"):
    if early_exit:
     with self.assertRaisesRegex(RuntimeError,"coverage incomplete"):p.collect(args)
    else:p.collect(args)
   result=json.loads((output/"result.json").read_text())
   if early_exit:
    self.assertFalse(result["full_timed_balance_covered"]);self.assertFalse((gate/"released.json").exists())
   else:
    self.assertTrue(result["full_timed_balance_covered"])
    for earlier,later in (("ack:enable","write:go.json"),("write:go.json","write:done.json"),("write:done.json","ack:disable"),("ack:disable","report"),("report","write:released.json")):
     self.assertLess(order.index(earlier),order.index(later))

 def test_complete_gate_order_covers_full_timer(self):self.run_mocked_collection()
 def test_profiler_exit_before_done_never_claims_full_coverage_or_releases(self):self.run_mocked_collection(True)


if __name__=="__main__":unittest.main()
