"""Three real CLI framing cases on fresh private copies, without insertion or save."""
import copy,hashlib,json,os,subprocess,time,unittest
from pathlib import Path
from .tree_manifest import artifact,canonical_bytes,file_hash
from .dynamic_runtime import copy_checkpoint,inventory

@unittest.skipUnless(os.environ.get('SPANN_PROTOCOL_NATIVE'),'reviewed exact CLI protocol fixture required')
class NativeSelectedWriteProtocol(unittest.TestCase):
    def test_exact_replay_conflict_and_eof_use_actual_cli(self):
        root=Path(os.environ['SPANN_PROTOCOL_OUTPUT']);root.mkdir()
        binary=Path(os.environ['SPANN_PROTOCOL_BINARY']);self.assertEqual(file_hash(binary),os.environ['SPANN_PROTOCOL_BINARY_SHA'])
        spec_path=Path(os.environ['SPANN_PROTOCOL_SOURCE_SPEC'])
        with spec_path.open('rb') as stream:raw=stream.read(64*1024+1)
        self.assertLessEqual(len(raw),64*1024);self.assertEqual(hashlib.sha256(raw).hexdigest(),os.environ['SPANN_PROTOCOL_SOURCE_SPEC_SHA'])
        original_spec=dict(path=str(spec_path.resolve()),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest());source=json.loads(raw)
        self.assertEqual(artifact(spec_path),original_spec)
        self.assertEqual(len(source['base']['owners']),1);self.assertEqual(source['base']['old_count'],18);self.assertEqual(source['base']['dimension'],4)
        parent=Path(source['base']['owners'][0]['checkpoint_dir']);before=inventory(parent);cases=[]
        for label in ['replay-abort','conflict','eof']:
            directory=root/label;directory.mkdir();checkpoint=directory/'checkpoint';copy_checkpoint(parent,checkpoint,32*1024**2)
            self.assertEqual(inventory(checkpoint),before)
            spec=copy.deepcopy(source);spec['base']['owners'][0]['checkpoint_dir']=str(checkpoint);path=directory/'spec.json';path.write_bytes(canonical_bytes(spec));spec_sha=file_hash(path)
            def frame(sequence,op):
                return canonical_bytes(dict(request_id=f"{spec['session_id']}:{spec['attempt_id']}:all:{sequence}",session_id=spec['session_id'],attempt_id=spec['attempt_id'],authorization=spec['authorization'],ownership_generation=spec['ownership_generation'],sequence=sequence,row_id=None,command=dict(op=op)))
            first=frame(1,'info')
            wire=first+first+frame(2,'abort') if label=='replay-abort' else first+b' '+first if label=='conflict' else first
            (directory/'stdin.jsonl').write_bytes(wire)
            command=[str(binary),'selected-write-session','--spec',str(path)];started=time.monotonic()
            try:
                run=subprocess.run(command,input=wire,capture_output=True,timeout=10,env=dict(os.environ,TOKIO_WORKER_THREADS='2',RUST_BACKTRACE='0'))
            except subprocess.TimeoutExpired as error:
                (directory/'stdout').write_bytes(error.stdout or b'');(directory/'stderr').write_bytes(error.stderr or b'');raise
            (directory/'stdout').write_bytes(run.stdout);(directory/'stderr').write_bytes(run.stderr)
            row=dict(label=label,command=command,returncode=run.returncode,wall_seconds=time.monotonic()-started,spec=artifact(path),stdin=artifact(directory/'stdin.jsonl'),stdout=artifact(directory/'stdout'),stderr=artifact(directory/'stderr'));cases.append(row);(root/'cases.json').write_bytes(canonical_bytes(cases))
            self.assertLess(len(run.stdout)+len(run.stderr),1024**2)
            lines=run.stdout.splitlines();replies=[json.loads(line) for line in lines]
            self.assertEqual(replies[0]['result']['op'],'info');self.assertEqual(replies[0]['result']['spec_sha256'],spec_sha)
            if label=='replay-abort':
                self.assertEqual(run.returncode,0);self.assertEqual(len(lines),3);self.assertEqual(lines[0],lines[1]);self.assertEqual(replies[2]['result']['op'],'aborted');self.assertEqual(replies[2]['sequence'],2)
            else:
                self.assertEqual(run.returncode,1);self.assertEqual(len(lines),1)
                self.assertIn(b'conflicting write replay' if label=='conflict' else b'write session ended without commit or explicit abort',run.stderr)
            self.assertEqual(inventory(checkpoint),before)
            self.assertFalse((checkpoint/f"manifest-{spec['base']['owners'][0]['generation']+1}.json").exists())
            self.assertEqual(inventory(parent),before);self.assertEqual(artifact(spec_path),original_spec)
        self.assertEqual([r['returncode'] for r in cases],[0,1,1])
        (root/'receipt.json').write_bytes(canonical_bytes(dict(scope='actual CLI Info/replay/Abort/conflict/EOF only; no Register or Commit',binary=artifact(binary),source_spec=original_spec,parent_inventory=before,cases=cases,no_saved_candidate=True)))
