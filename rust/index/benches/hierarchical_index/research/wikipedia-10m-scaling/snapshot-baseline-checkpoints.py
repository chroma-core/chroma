import json, subprocess, time
from pathlib import Path
results=Path('/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008')
source=Path('/mnt/data/wikipedia-10m-scaling-baseline-20261008')
out=Path('/mnt/data/wikipedia-10m-scaling-baseline-snapshots-20261008')
out.mkdir(exist_ok=True)
seen=set()
while True:
    try:
        meta=json.loads((source/'checkpoint.json').read_text())
        n=meta['total_vectors']
        if n not in seen:
            dest=out/str(n)
            start=time.monotonic()
            subprocess.run(['cp','-al',str(source),str(dest)],check=True)
            saved=json.loads((dest/'checkpoint.json').read_text())
            assert saved==meta, (saved,meta)
            print(f"{n} vectors snapshot saved in {time.monotonic()-start:.3f}s",flush=True)
            seen.add(n)
    except FileNotFoundError:
        pass
    if (results/'exit-status').exists():
        break
    time.sleep(1)
