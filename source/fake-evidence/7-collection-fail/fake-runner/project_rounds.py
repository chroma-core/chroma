"""Project ordered corpus rounds into owner batches and cumulative ID maps.

This prepares files only. It never starts a builder, downloads data, or changes
an existing checkpoint. Assignment labels must come from the frozen model.
"""
import hashlib
import json
from pathlib import Path
import struct


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def project(vectors, labels, output, dimension, owners, round_rows):
    vectors, labels, output = map(Path, (vectors, labels, output))
    if min(dimension, owners, round_rows) < 1:
        raise ValueError('positive dimensions, owners and round size required')
    row_bytes = dimension * 4
    size = vectors.stat().st_size
    if not size or size % row_bytes:
        raise ValueError('incomplete vector rows')
    rows = size // row_bytes
    if labels.stat().st_size != rows * 4:
        raise ValueError('one uint32 owner label required per row')
    output.mkdir(parents=True, exist_ok=False)
    counts = [0] * owners
    previous_maps = [None] * owners
    previous_hashes = [None] * owners
    jobs = []
    with vectors.open('rb') as source, labels.open('rb') as assignments:
        for start in range(0, rows, round_rows):
            generation = start // round_rows + 1
            folder = output / f'round-{generation:04d}'
            folder.mkdir()
            streams = {}; new_ids = {}
            try:
                for gid in range(start, min(rows, start + round_rows)):
                    row = source.read(row_bytes)
                    owner = struct.unpack('<I', assignments.read(4))[0]
                    if len(row) != row_bytes or owner >= owners:
                        raise ValueError('truncated row or invalid owner')
                    if owner not in streams:
                        streams[owner] = (folder / f'{owner}.f32').open('xb')
                        new_ids[owner] = []
                    streams[owner].write(row)
                    new_ids[owner].append(gid)
            finally:
                for stream in streams.values():
                    stream.close()
            for owner in sorted(new_ids):
                ids = new_ids[owner]
                target = folder / f'{owner}.u64le'
                with target.open('xb') as out:
                    previous = previous_maps[owner]
                    if previous is not None:
                        if digest(previous) != previous_hashes[owner]:
                            raise ValueError('prior cumulative map changed')
                        with previous.open('rb') as old:
                            for block in iter(lambda: old.read(1024 * 1024), b''):
                                out.write(block)
                    out.write(struct.pack('<' + 'Q' * len(ids), *ids))
                batch = folder / f'{owner}.f32'
                jobs.append(dict(owner=owner, generation=generation,
                                 global_round_start=start, batch_rows=len(ids),
                                 prior_rows=counts[owner], resume=counts[owner] > 0,
                                 checkpoint_rows=len(ids), vectors=str(batch),
                                 vectors_sha256=digest(batch), id_map=str(target),
                                 id_map_sha256=digest(target),
                                 prior_map_sha256=previous_hashes[owner]))
                counts[owner] += len(ids)
                previous_maps[owner] = target
                previous_hashes[owner] = jobs[-1]['id_map_sha256']
    if sum(counts) != rows or any(n == 0 for n in counts):
        raise ValueError('every declared owner must have final rows')
    result = dict(rows=rows, dimension=dimension, owners=owners,
                  round_rows=round_rows, owner_counts=counts, jobs=jobs,
                  vectors_sha256=digest(vectors), labels_sha256=digest(labels))
    (output / 'projection.json').write_text(json.dumps(result, indent=2) + '\n')
    return result
