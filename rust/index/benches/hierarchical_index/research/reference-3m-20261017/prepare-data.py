import concurrent.futures, hashlib, json, os
from pathlib import Path
from huggingface_hub import hf_hub_download
import pyarrow.parquet as pq
import numpy as np
root=Path('/home/ubuntu/reference3m')
revision='ade45fb52bd549f5e8c065636fe4160a43c2af36'
repo='CohereLabs/wikipedia-2023-11-embed-multilingual-v3'
cache=root/'hf/hub'
def fetch(i):
    name=f'en/{i:04d}.parquet'
    path=Path(hf_hub_download(repo_id=repo,repo_type='dataset',filename=name,revision=revision,cache_dir=cache))
    sha=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(8*1024*1024),b''):sha.update(block)
    rows=pq.ParquetFile(path).metadata.num_rows
    assert rows==100000,(name,rows)
    print(f'{name}: {rows} rows, sha256={sha.hexdigest()}',flush=True)
    return dict(path=str(path),rows=rows,sha256=sha.hexdigest())
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:shards=list(pool.map(fetch,range(30)))
# Make the benchmark's default revision resolve to these exact pinned cached files.
ref=cache/'datasets--CohereLabs--wikipedia-2023-11-embed-multilingual-v3/refs/main'
ref.parent.mkdir(parents=True,exist_ok=True);ref.write_text(revision)
vector_hash=hashlib.sha256();prefixes={}
count=0
for shard in shards:
    for batch in pq.ParquetFile(shard['path']).iter_batches(batch_size=10000,columns=['emb']):
        arr=batch.column(0);assert arr.null_count==0
        assert np.all(np.diff(np.asarray(arr.offsets))==1024)
        x=np.asarray(arr.values.to_numpy(zero_copy_only=False),dtype='<f4').reshape(-1,1024)
        assert np.isfinite(x).all();vector_hash.update(x.tobytes());count+=len(x)
        if count%1000000==0:prefixes[str(count)]=vector_hash.hexdigest()
assert count==3000000
manifest=dict(repository=repo,revision=revision,shards=shards,rows=count,dimension=1024,ordered_little_endian_f32_prefix_sha256=prefixes)
(root/'evidence/source-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print('Pinned 3M source verified',flush=True)
