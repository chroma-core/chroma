"""Freeze the reviewed source and exact file inventory once before remote build."""
from pathlib import Path
import json,hashlib,tarfile,datetime
p=Path(__file__).resolve().parent;source=Path('/private/tmp/spann-research34')
control=json.loads((p.parent/'development32/epoch32v2-source-manifest.json').read_text())
files={str(f.relative_to(source)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(source.rglob('*')) if f.is_file() and not any(x in f.relative_to(source).parts for x in ['.git','target','__pycache__'])}
changed=[name for name,h in files.items() if control['source_files'].get(name)!=h]
manifest={**control,'source':'/home/ubuntu/spann-two-machine-v1/development30/source34v1','source_files':files,'source_root':str(source),'frozen_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'changed_files_from_epoch32':changed,'combined_review_passed':True}
archive=p.parent.parent/'packages/source-epoch34v1.tar.gz';assert not archive.exists()
with tarfile.open(archive,'w:gz') as tar:
 for name in files:tar.add(source/name,arcname=name,recursive=False)
manifest['archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest()
(p/'epoch34v1-source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');print(json.dumps({'changed':changed,'files':len(files),'archive_sha256':manifest['archive_sha256']},indent=2))
