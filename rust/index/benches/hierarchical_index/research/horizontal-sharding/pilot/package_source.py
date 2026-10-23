"""Snapshot tracked and untracked nonignored source, including tracked directory symlinks."""
import subprocess,tarfile,pathlib,hashlib,json,time
root=pathlib.Path('/private/tmp/chroma-hierarchical-spann-sharding');out=pathlib.Path(__file__).parent
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=root).decode().split('\0')
with tarfile.open(out/'source.tar.gz','w:gz') as tar:
 for name in files:
  if name and ((root/name).is_file() or (root/name).is_symlink()):tar.add(root/name,arcname='source/'+name,recursive=False)
p=out/'source.tar.gz';(out/'source-manifest.json').write_text(json.dumps({'base':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'archive_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'file_count':len(files)-1,'created_unix':time.time()},indent=2)+'\n')
