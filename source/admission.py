"""Reused saved-generation admission and bounded output helpers."""
import json,hashlib,time,struct
from pathlib import Path
from harness_common import require,verify,descriptor
from quality_analysis import DIM,ROWS
OVERLAY_VERIFY={"calls":0,"code_hash_read_bytes":0,"manifest_read_bytes":0,"elapsed_seconds":0.}

def inventory(store,deadline):
    result={};total=0
    for path in sorted(store.rglob('*')):
        require(time.time()<deadline,'inventory deadline');require(not path.is_symlink(),'saved store symlink')
        if not path.is_file():continue
        require(len(result)<10000,'saved object cap');size=path.stat().st_size;total+=size;require(total<=512*1024**2,'saved store payload cap');h=hashlib.sha256()
        with path.open('rb') as f:
            for data in iter(lambda:f.read(1024*1024),b''):require(time.time()<deadline,'inventory deadline');h.update(data)
        result[str(path.relative_to(store))]=dict(bytes=size,sha256=h.hexdigest())
    require(result,'empty saved store');return dict(files=result,logical_hash_read_bytes=total,scope='Whole saved-store before/after byte identity. Original manifest/map hashes are previously admitted; this new inventory is not a backdated independent object-hash receipt.')


def input_snapshot(spec,deadline):
    manifest=json.loads(verify(spec['manifest']).read_text());mapping=verify(spec['mapping'])
    require(manifest['generation']==1 and manifest['dimension']==DIM and manifest['metric']=='ip' and manifest['vector_count']==ROWS and manifest['shard_id']=='global','saved manifest shape')
    require(manifest['block_ids']==spec['roots'] and manifest['integrity_audit']==spec['census'],'original roots/census')
    require(manifest['id_mapping']['sha256']==spec['mapping']['sha256'] and manifest['id_mapping']['count']==ROWS and manifest['id_mapping']['bytes']==ROWS*8 and manifest['id_mapping']['path']==mapping.name,'original map descriptor')
    require(struct.unpack('<'+str(ROWS)+'Q',mapping.read_bytes())==tuple(range(ROWS)),'dense identity map')
    census=spec['census'];require(census['unique_live_rows']==ROWS and census['duplicate_current_entries']==census['unreachable_nodes']==0,'admitted complete census')
    return manifest,inventory(Path(spec['manifest']['path']).parent,deadline)


def serialized_result(result,metadata_before):
    # Include the reporting field's own bytes. Its decimal width reaches a fixed
    # point after serialization, so the reported total equals the retained bytes.
    result['metadata_including_result_bytes']=0
    for _ in range(16):
        payload=(json.dumps(result,indent=2)+'\n').encode('utf-8')
        total=metadata_before+len(payload)
        if total==result['metadata_including_result_bytes']:return payload
        result['metadata_including_result_bytes']=total
    raise ValueError('metadata serialization did not converge')


def write_final_result(out,result,metadata_before,cap=512*1024**2):
    payload=serialized_result(result,metadata_before)
    if metadata_before+len(payload)>cap:
        result.update(status='failed',error='metadata cap exceeded')
        payload=serialized_result(result,metadata_before)
    with (out/'result.json').open('xb') as stream:stream.write(payload)
    require(result['metadata_including_result_bytes']==metadata_before+(out/'result.json').stat().st_size,'actual metadata size')
    return result


def overlay_closure(path,snapshot):
    started=time.monotonic();path=Path(path);manifest=path/'overlay.json';require(manifest.stat().st_size<=512*1024,'overlay metadata cap');m=json.loads(manifest.read_text())
    require(m['policy']=='code4-residual-postings-v1' and m['snapshot']==snapshot['manifest']['sha256'] and m['dimension']==768 and m['metric']=='InnerProduct' and m['root']==snapshot['root_ref']['id'],'overlay snapshot binding')
    require(len(m['leaves'])==snapshot['census']['reachable_leaves']<=256,'overlay leaf closure')
    files={};total=0
    for leaf in m['leaves']:
        name=leaf['file'];require(name==f"leaf-{leaf['id']}.code4" and name not in files,'overlay local path/uniqueness');require(type(leaf['rows']) is int and type(leaf['unique_rows']) is int and 0<=leaf['unique_rows']<=leaf['rows']<=4096,'overlay row bounds');p=path/name;require(not p.is_symlink(),'overlay symlink');pin=descriptor(p);require(pin['bytes']==leaf['bytes']==leaf['unique_rows']*400 and pin['sha256']==leaf['sha256'],'overlay leaf digest/extent');files[name]=pin;total+=pin['bytes']
    require(total<=16*1024**2+32768*4,'overlay payload bound');OVERLAY_VERIFY['calls']+=1;OVERLAY_VERIFY['code_hash_read_bytes']+=total;OVERLAY_VERIFY['manifest_read_bytes']+=manifest.stat().st_size*2;OVERLAY_VERIFY['elapsed_seconds']+=time.monotonic()-started;return dict(manifest=descriptor(manifest),codes=files,serialized_code_bytes=total)

