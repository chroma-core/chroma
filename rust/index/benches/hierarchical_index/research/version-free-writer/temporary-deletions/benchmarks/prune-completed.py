import json,shutil,time,hashlib
from pathlib import Path
r=Path('/mnt/data/temporary-deletions-20261020');a=r/'artifacts';deleted=[]
for variant in ['reference','candidate']:
 name=f'wikipedia-{variant}-9m-1';out=r/name
 assert out.is_dir() and not out.is_symlink() and out.resolve().parent==r
 result=json.loads((a/f'{name}.result.json').read_text());assert result['returncode']==0 and result['output_realpath']==str(out)
 for suffix in ['audit-result.json','audit-extended-result.json']:
  assert json.loads((a/f'{name}.{suffix}').read_text())['returncode']==0
 ck=out/'index/checkpoint.json';assert ck.read_bytes()==(a/f'{name}.checkpoint.json').read_bytes()
 assert json.loads(ck.read_text())['total_vectors']==9000000
 deleted.append({'path':str(out),'checkpoint_sha256':hashlib.sha256(ck.read_bytes()).hexdigest(),'audit_passed':True})
 shutil.rmtree(out)
(a/'pruned-completed-copies.json').write_text(json.dumps({'deleted':deleted,'finished_unix':time.time(),'scope':'Only completed isolated9M benchmarkcopies; original8M and original40M/MSMARCO untouched'},indent=2)+'\n')
