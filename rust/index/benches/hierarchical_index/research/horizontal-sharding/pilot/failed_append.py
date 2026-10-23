"""Fail after a private checkpoint on one remote shard; preserve published generation."""
import pathlib,struct,subprocess,json,math
root=pathlib.Path('/mnt/spann/smoke/0');before=json.loads((root/'manifest.json').read_text());assert before['generation']==2
p=root/'failed-append.f32';p.write_bytes(struct.pack('<6f',.1,0,.2,0,float('nan'),0))
command=['/mnt/spann/hierarchical_spann_shard','build','--vectors',str(p),'--dimension','2','--global-id-start','6','--id-stride','2','--row-count','3','--checkpoint-dir',str(root),'--shard-id','0','--checkpoint-rows','1','--generation','3','--source-id','two-host-smoke','--metric','l2','--resume']
r=subprocess.run(command,capture_output=True,text=True,timeout=30)
assert r.returncode!=0,(r.stdout,r.stderr)
assert json.loads((root/'manifest.json').read_text())==before
assert not (root/'manifest-3.json').exists()
out=pathlib.Path('/mnt/spann/results/failed-append');out.mkdir(exist_ok=True)
(out/'stdout.log').write_text(r.stdout);(out/'stderr.log').write_text(r.stderr)
(out/'result.json').write_text(json.dumps(dict(status='passed',returncode=r.returncode,check='Failure after private checkpoint leaves published generation2 unchanged',generation=2),indent=2)+'\n')
print((out/'result.json').read_text())
