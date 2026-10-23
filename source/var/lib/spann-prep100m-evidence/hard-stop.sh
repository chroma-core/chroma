#!/bin/bash
set +e
timeout 20 python3 - <<'PY'
from pathlib import Path
import shutil,json,time
out=Path('/var/lib/spann-prep100m-evidence/final');out.mkdir(exist_ok=True);used=0;copied=[]
root=Path('/mnt/spann/prep100m-v1')
for folder in ['reports','archive','control']:
 for p in (root/folder).rglob('*'):
  if p.is_symlink() or not p.is_file() or p.suffix not in ('.json','.jsonl','.log'):continue
  size=p.stat().st_size
  if size>8*1024**2 or used+size>64*1024**2:continue
  target=out/p.relative_to(root);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target);used+=size;copied.append(str(p))
p=root/'pipeline.json'
if p.exists() and p.stat().st_size<=8*1024**2 and used+p.stat().st_size<=64*1024**2:shutil.copyfile(p,out/'pipeline.json')
(out/'receipt.json').write_text(json.dumps(dict(epoch=time.time(),bytes=used,copied=copied)))
PY
timeout 5 sync
shutdown -h now
