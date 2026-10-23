"""Materialize a real prefix and append from previously hashed modulo partitions."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import numpy as np


def prepare(parent, output, base_rows=100000, append_rows=1000):
    parent, output = Path(parent), Path(output)
    info = json.loads((parent/'input.json').read_text())
    shards = len(info['shards'])
    dimension = info['dimension']
    if info['start_id'] != 0 or min(base_rows, append_rows) <= 0 or base_rows % shards or append_rows % shards:
        raise ValueError('require positive shard-divisible prefix and append sizes from zero')
    if base_rows + append_rows > info['row_count']:
        raise ValueError('subset exceeds parent corpus')
    output.mkdir(parents=True, exist_ok=False)
    begun = time.monotonic()
    sources = [np.memmap(parent/f'shard-{i}.f32', mode='r', dtype='<f4', shape=(s['row_count'], dimension)) for i, s in enumerate(info['shards'])]
    products = []
    for name, start, count in [('base', 0, base_rows), ('append', base_rows, append_rows)]:
        folder = output/name
        folder.mkdir()
        local_start, local_count = start//shards, count//shards
        for i, source in enumerate(sources):
            path = folder/f'shard-{i}.f32'
            h = hashlib.sha256()
            with path.open('wb') as out:
                for offset in range(local_start, local_start+local_count, 1024):
                    block = source[offset:min(offset+1024, local_start+local_count)].tobytes()
                    out.write(block); h.update(block)
            products.append(dict(kind=name, shard_id=i, rows=local_count, global_id_start=start+i, id_stride=shards, path=str(path), bytes=path.stat().st_size, sha256=h.hexdigest()))
        # Reconstruct the dense original row order for the independent exact scorer.
        with (output/f'{name}.f32').open('wb') as out:
            for offset in range(local_start, local_start+local_count, 1024):
                end = min(offset+1024, local_start+local_count)
                block = np.empty(((end-offset)*shards, dimension), dtype='<f4')
                for i, source in enumerate(sources):
                    block[i::shards] = source[offset:end]
                out.write(block.tobytes())
    manifest = dict(parent_source_sha256=info['source_sha256'], parent_source_id=info['source_id'], dimension=dimension,
                    base_rows=base_rows, append_rows=append_rows, shard_count=shards, outputs=products, preparation_wall_s=time.monotonic()-begun)
    (output/'provenance.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--parent', required=True); p.add_argument('--output', required=True)
    p.add_argument('--base-rows', type=int, default=100000); p.add_argument('--append-rows', type=int, default=1000)
    a = p.parse_args(); print(json.dumps(prepare(a.parent, a.output, a.base_rows, a.append_rows)))

if __name__ == '__main__':
    main()
