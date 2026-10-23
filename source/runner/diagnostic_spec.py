"""Bind final saved snapshots to the accepted read-only quality diagnostic."""
import json
import struct
from pathlib import Path
from project_rounds import digest


def artifact(path):
    path = Path(path).resolve()
    return dict(path=str(path), bytes=path.stat().st_size, sha256=digest(path))


def create(projection_path, build_output, queries, reference, output, source_id):
    projection = json.loads(Path(projection_path).read_text())
    query_rows = [json.loads(x) for x in Path(queries).read_text().splitlines() if x.strip()]
    if len(query_rows) != 16 or len({q['query_id'] for q in query_rows}) != 16:
        raise ValueError('sixteen distinct selected queries required')
    last = {}
    for job in projection['jobs']: last[job['owner']] = job
    if set(last) != set(range(projection['owners'])):
        raise ValueError('missing final owner')
    owners = []; maps = set()
    seen = bytearray(projection['rows']); total = 0
    for owner, job in sorted(last.items()):
        snapshot = Path(build_output)/'indexes'/str(owner)/f"manifest-{job['generation']}.json"
        m = json.loads(snapshot.read_text())
        if m['source_id'] != source_id or m['metric'] != 'ip' or m['dimension'] != projection['dimension']:
            raise ValueError('snapshot cohort mismatch')
        if m['vector_count'] != projection['owner_counts'][owner] or m['generation'] != job['generation']:
            raise ValueError('snapshot count/generation mismatch')
        mapping = m['id_mapping']
        map_path = Path(mapping['path'])
        if not map_path.is_absolute(): map_path = snapshot.parent / map_path
        if mapping['sha256'] != job['id_map_sha256'] or digest(map_path) != mapping['sha256']:
            raise ValueError('saved map differs from projected cumulative map')
        if mapping['count'] != m['vector_count'] or map_path.stat().st_size != m['vector_count'] * 8:
            raise ValueError('saved map length mismatch')
        with map_path.open('rb') as stream:
            for raw in iter(lambda: stream.read(65536), b''):
                if len(raw) % 8: raise ValueError('truncated map')
                for (gid,) in struct.iter_unpack('<Q', raw):
                    if gid >= len(seen) or seen[gid]: raise ValueError('invalid or repeated global ID')
                    seen[gid] = 1; total += 1
        if job['id_map_sha256'] in maps: raise ValueError('duplicate owner map identity')
        maps.add(job['id_map_sha256'])
        owners.append(dict(owner_id=str(owner),generation=job['generation'],snapshot=artifact(snapshot)))
    if total != projection['rows'] or not all(seen):
        raise ValueError('incomplete global map union')
    spec = dict(format_version=1,source_id=source_id,dimension=projection['dimension'],metric='ip',
                global_rows=projection['rows'],max_map_rows=projection['rows'],
                ground_truth=artifact(reference),query_vectors=artifact(queries),
                selected_queries=[dict(query_id=q['query_id'],expected=100) for q in query_rows],owners=owners)
    with Path(output).open('x') as stream: stream.write(json.dumps(spec,indent=2)+'\n')
    return spec
