import pathlib, shutil, json
src=pathlib.Path('/mnt/data/wikipedia-10m-scaling-snapshots-20261008/9000000')
dst=pathlib.Path('/mnt/data/large-index-investigation-components-9m')
count=0
for p in (src/'data/block').iterdir():
 target=dst/'data/block'/p.name
 if not target.exists():
  shutil.copy2(p,target)
  assert p.stat().st_ino != target.stat().st_ino
  count+=1
print('Added independent embedding blocks:',count,flush=True)
