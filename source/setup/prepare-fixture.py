"""Generate a small deterministic IP fixture and explicitly admitted stage specs.

This writes files only. It never starts a process, machine, or execution window.
"""
import argparse
import hashlib
import json
from pathlib import Path
import random
import struct


def artifact(path):
    path = Path(path).resolve()
    return dict(path=str(path), bytes=path.stat().st_size,
                sha256=hashlib.sha256(path.read_bytes()).hexdigest())


def write(path, data):
    with path.open('x') as stream:
        json.dump(data, stream, indent=2)
        stream.write('\n')


def prepare(root, base):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    rng = random.Random(1729)
    rows, dimension = 128, 768
    vectors = [[rng.uniform(-1, 1) for _ in range(dimension)] for _ in range(rows)]
    with (root/'vectors.f32').open('xb') as stream:
        for row in vectors:
            stream.write(struct.pack('<'+'f'*dimension, *row))
    # Query coordinates are rounded to the exact same FP32 representation.
    with (root/'queries.jsonl').open('x') as stream:
        for i in range(16):
            row = list(struct.unpack('<'+'f'*dimension, struct.pack('<'+'f'*dimension, *vectors[i])))
            stream.write(json.dumps({'query_id': str(i), 'vector': row})+'\n')
    common = dict(format_version=1, source_id='controlled-global128-ip-v1',
                  vectors=artifact(root/'vectors.f32'), rows=rows, dimension=dimension, metric='ip',
                  config=dict(branching_factor=4, split_threshold=16, merge_threshold=4,
                              write_beam_tau=1.5, write_beam_min=10, write_beam_max=16,
                              write_rng_epsilon=0.0, write_rng_factor=1.0,
                              reassign_neighbor_count=32, write_navigation='fp', fp_npa=True),
                  storage_chunk=13, balance_every=32, owner_slots=2, owner_vector_threshold=48,
                  max_nodes=4096, max_owner_physical_rows=4096, max_leaf_rows=1024)
    stage = dict(base, vectors=common['vectors'], queries=artifact(root/'queries.jsonl'),
                 output=str(root/'prepared'), rows=rows, dimension=dimension)
    write(root/'prepare-stage.json', stage)
    write(root/'native-common.json', common)
    write(root/'fixture.json', {'rows': rows, 'dimension': dimension, 'metric': 'ip',
                              'queries': 16, 'top_k': 100, 'seed': 1729,
                              'purpose': 'Connected runner correctness; no real-data quality or speed claim',
                              'input_bytes': rows*dimension*4})


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--admitted-base', required=True)
    args = parser.parse_args()
    prepare(args.output, json.loads(Path(args.admitted_base).read_text()))
