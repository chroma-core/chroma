"""Validate a global construction manifest without rewriting the saved store."""
import hashlib
import json
from pathlib import Path
import struct
import uuid

EXPECTED_CONFIG = dict(branching_factor=100, split_threshold=2048, merge_threshold=512,
    write_beam_tau=1.5, write_beam_min=10, write_beam_max=16, write_level_taus=[],
    write_level_min_pcts=[], max_replicas=1, write_rng_epsilon=0.0, write_rng_factor=1.0,
    reassign_neighbor_count=32, write_navigation='fp', fp_npa=True)
ROOTS = {'internal_node_id','leaf_node_id','posting_list_id','scalar_metadata_id','vector_data_id'}

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def checked(pin):
    p=Path(pin['path'])
    if p.is_symlink() or not p.is_file() or p.stat().st_size!=pin['bytes'] or digest(p)!=pin['sha256']:
        raise ValueError('descriptor integrity mismatch')
    return p

def bridge(pin, expected_rows=2000000):
    p=checked(pin);m=json.loads(p.read_text())
    if m['format_version']!=1 or m['rows']!=expected_rows or m['dimension']!=768 or m['metric']!='ip' or m['treatment']!='reference':
        raise ValueError('unsupported reference identity')
    effective=dict(EXPECTED_CONFIG);effective.update(m['spec']['config'])
    if m['config']!=EXPECTED_CONFIG or effective!=EXPECTED_CONFIG:
        raise ValueError('construction configuration differs')
    if set(m['block_ids'])!=ROOTS or len({str(uuid.UUID(v)) for v in m['block_ids'].values()})!=5:
        raise ValueError('invalid immutable root set')
    a=m['integrity_audit']
    if a['unique_live_rows']!=expected_rows or a['duplicate_current_entries'] or a['unreachable_nodes'] or a['reachable_nodes']!=a['stored_nodes']:
        raise ValueError('saved audit binding fails')
    mp=m['id_mapping'];rel=Path(mp['path'])
    if rel.is_absolute() or '..' in rel.parts or len(rel.parts)!=1:
        raise ValueError('unsafe map path')
    store=p.parent/'store';mapping=store/rel
    if not store.is_dir() or store.is_symlink() or mapping.is_symlink():raise ValueError('saved store path absent/unsafe')
    if mp['format']!='dense-u64-le-identity' or mp['rows']!=expected_rows or mp['count']!=expected_rows or mp['bytes']!=8*expected_rows or mp['global_start']!=0 or mp['local_start']!=0:
        raise ValueError('mapping declaration differs')
    checked(dict(path=str(mapping),bytes=mp['bytes'],sha256=mp['sha256']))
    with mapping.open('rb') as stream:
        i=0
        while data:=stream.read(8192*8):
            if len(data)%8:raise ValueError('truncated map')
            for (value,) in struct.iter_unpack('<Q',data):
                if value!=i:raise ValueError('nonidentity map')
                i+=1
    if i!=expected_rows:raise ValueError('map count differs')
    return dict(protocol='global-reference-readonly-bridge-v1',manifest=pin,store=str(store),rows=expected_rows,
                dimension=768,metric='ip',config=m['config'],block_ids=m['block_ids'],mapping=dict(path=str(mapping),bytes=mp['bytes'],sha256=mp['sha256']),
                expected_root=m['root'],saved_audit=a,scope='Validated saved audit receipt; actual reader topology must still match. No store rewrite.')
