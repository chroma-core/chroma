import pathlib, json, uuid, shutil, pyarrow.ipc as ipc, hashlib
root=pathlib.Path('/mnt/data')
out=pathlib.Path('/home/ubuntu/benchmark-data/large-index-balancing-investigation')
source=root/'wikipedia-40m-current'
def manifest(p):
 files=sorted((str(f.relative_to(p)), f.stat().st_size, f.stat().st_mtime_ns, f.stat().st_ino) for f in p.rglob('*') if f.is_file())
 return {'checkpoint':json.loads((p/'checkpoint.json').read_text()),'files':files}
(out/'40m-before.json').write_text(json.dumps(manifest(source)))
for size, src in [(40,source),(9,root/'wikipedia-10m-scaling-snapshots-20261008/9000000')]:
 dst=root/f'large-index-investigation-components-{size}m'
 assert not dst.exists(), dst
 (dst/'data/block').mkdir(parents=True)
 (dst/'data/sparse_index').mkdir()
 meta=json.loads((src/'checkpoint.json').read_text())
 shutil.copy2(src/'checkpoint.json',dst/'checkpoint.json')
 copied=set()
 for name in ['scalar_metadata_id','posting_list_id','leaf_node_id','internal_node_id','vector_data_id']:
  si=src/'data/sparse_index'/meta[name]
  shutil.copy2(si,dst/'data/sparse_index'/si.name)
  table=ipc.open_file(si).read_all().to_pydict()
  for prefix, raw in zip(table['prefix'],table['id']):
   if name=='vector_data_id' and prefix not in ['START','centroid']: continue
   bid=str(uuid.UUID(bytes=raw))
   if bid not in copied:
    shutil.copy2(src/'data/block'/bid,dst/'data/block'/bid)
    assert (src/'data/block'/bid).stat().st_ino != (dst/'data/block'/bid).stat().st_ino
    copied.add(bid)
 print(size,len(copied),sum(f.stat().st_size for f in (dst/'data/block').iterdir()),flush=True)
(out/'component-fixtures.json').write_text(json.dumps({'40m':str(root/'large-index-investigation-components-40m'),'9m':str(root/'large-index-investigation-components-9m')}))
