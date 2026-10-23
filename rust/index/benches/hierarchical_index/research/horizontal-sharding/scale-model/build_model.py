"""Derive bounded arithmetic and placement-count models from preserved reports.

This reads existing local evidence. It runs no vector search, benchmark, cloud
operation, or source transfer, and never extracts an archive onto disk.
"""
import hashlib
import json
import math
from pathlib import Path
import statistics
import tarfile

ROOT=Path(__file__).resolve().parents[1]
RESULTS=ROOT/'pilot'/'results'


def digest(path):
    result=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(1024*1024),b''):result.update(block)
    return result.hexdigest()


def storage():
    metadata=RESULTS/'fineweb10m-read-immutable-metadata-v1.tar.gz';tree=None
    with tarfile.open(metadata,'r|gz') as stream:
        for member in stream:
            if member.isfile() and member.name.endswith('tree-1.json'):
                tree=json.load(stream.extractfile(member));break
    tree_hash=hashlib.sha256((json.dumps(tree,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()).hexdigest()
    summary=json.loads((RESULTS/'fineweb10m-selected-storage-summary-v1.json').read_text())
    calibration=json.loads((RESULTS/'fineweb10m-calibration-independent-review.json').read_text())
    if tree_hash!=summary['tree_sha256'] or tree_hash!=calibration['tree_sha256']:raise ValueError('storage/calibration tree epochs differ')
    slots={row['subtree_id']:row for row in tree['subtrees']}
    rows={};archive=RESULTS/'fineweb10m-selected-storage-small-v1.tar.gz'
    with tarfile.open(archive,'r|gz') as stream:
        for member in stream:
            if member.isfile() and member.name.endswith('.json'):
                if member.size>8*1024**2:raise ValueError('metadata report exceeds bounded read')
                row=json.load(stream.extractfile(member))
                if not isinstance(row,dict) or row.get('exit_status')!=0 or 'report' not in row:continue
                report=row['report'];current=next(g for g in report['generations'] if g['generation']==report['current_generation'])
                sid=row['subtree_id']
                if sid in rows:raise ValueError('duplicate owner report')
                if report['current_manifest_sha256']!=slots[sid]['snapshot']['sha256'] or current['vector_count']!=slots[sid]['vector_count'] or current['id_mapping']['sha256']!=slots[sid]['local_to_global']['sha256']:raise ValueError('owner snapshot/map differs from exact selected tree')
                rows[sid]=dict(rows=current['vector_count'],live_bytes=report['current_live_objects']['logical_file_bytes'],physical_bytes=report['physical_object_store']['logical_file_bytes'],flushes=math.ceil(current['vector_count']/10000))
    if len(rows)!=128 or sum(row['rows'] for row in rows.values())!=10000000:raise ValueError('incomplete10M owner inventory')
    return rows,archive,tree_hash


def placement(rows,hosts):
    loads=[0]*hosts;adapters=[[0]*4 for _ in range(hosts)];mapping={}
    for sid,row in sorted(rows.items(),key=lambda pair:(-pair[1]['physical_bytes'],pair[0])):
        host=min(range(hosts),key=lambda h:(loads[h],h));adapter=min(range(4),key=lambda a:(adapters[host][a],a))
        loads[host]+=row['physical_bytes'];adapters[host][adapter]+=row['physical_bytes'];mapping[sid]=(host,adapter)
    return mapping,loads


def phases(result):
    grouped=[]
    for level in result['levels']:
        owners={node['subtree_id'] for node in level['selected'] if not node['is_leaf']}
        if owners:grouped.append(owners)
    for operation in ('score_selected_leaves','rerank_selected'):
        owners={row['subtree_id'] for row in result['worker_metrics'] if row['op']==operation}
        if owners:grouped.append(owners)
    return grouped


def main():
    rows,storage_archive,tree_hash=storage();sizes=[row['rows'] for row in rows.values()]
    policy=json.loads((RESULTS/'fineweb10m-calibration-independent-review.json').read_text())['points']['calibration-b2048-m1000.jsonl']
    mappings={hosts:placement(rows,hosts) for hosts in (1,2,4,8,16,32)}
    totals={hosts:dict(rpc_batches=[],wire_candidates=[],phase_count=[]) for hosts in mappings};count=0;cache={};input_archive=RESULTS/'fineweb10m-calibration-small-v1.tar.gz'
    with tarfile.open(input_archive,'r|gz') as stream:
        for member in stream:
            if not member.isfile() or not member.name.endswith('calibration-b2048-m1000.jsonl'):continue
            source=stream.extractfile(member)
            while True:
                line=source.readline(32*1024**2+1)
                if not line:break
                if len(line)>32*1024**2:raise ValueError('query record exceeds bounded read')
                record=json.loads(line)
                if record['status']!='ok':raise ValueError('failed query cannot enter model')
                result=record['result'];local={}
                for item in result['worker_metrics']:
                    metrics=item['metrics'];sid=item['subtree_id'];retained=metrics.get('retained_postings')
                    if retained:
                        old=cache.setdefault(sid,dict(entries=0,owned_bytes=0))
                        old['entries']=max(old['entries'],retained['entries']);old['owned_bytes']=max(old['owned_bytes'],retained['leaf_owned_bytes_including_centroids'])
                    if item['op']=='score_selected_leaves':
                        local[sid]=metrics['candidate_contributions_before_owner_reduction']
                        if not 0<=local[sid]<=min(1000,metrics['unique_candidates_scanned']):raise ValueError('invalid unique local contribution count')
                if sum(local.values())!=result['candidate_contributions']:raise ValueError('logical work counter differs')
                phase_owners=phases(result)
                if sum(len(owners) for owners in phase_owners)!=len(result['worker_metrics']):raise ValueError('replayed phase operations differ from actual logical calls')
                for hosts,(mapping,loads) in mappings.items():
                    batches=sum(len({mapping[sid] for sid in owners}) for owners in phase_owners)
                    groups={}
                    for sid,n in local.items():groups[mapping[sid]]=groups.get(mapping[sid],0)+n
                    wire=sum(min(1000,n) for n in groups.values())
                    totals[hosts]['rpc_batches'].append(batches);totals[hosts]['wire_candidates'].append(wire);totals[hosts]['phase_count'].append(len(phase_owners))
                count+=1
            break
    if count!=100:raise ValueError('selected calibration record count differs')
    for sid,values in cache.items():rows[sid].update(cache_max_entries=values['entries'],cache_max_owned_bytes=values['owned_bytes'])
    live=sum(row['live_bytes'] for row in rows.values());physical=sum(row['physical_bytes'] for row in rows.values());owned=sum(row['owned_bytes'] for row in cache.values())
    model=dict(tree_sha256=tree_hash,analysis_source_sha256=digest(Path(__file__)),scope='Offline arithmetic and exact contribution-count replay of existing100query traces; no new benchmark or query-quality result.',evidence_rows=10000000,owners=len(rows),owner_rows=dict(min=min(sizes),mean=statistics.mean(sizes),max=max(sizes),max_over_mean=max(sizes)/statistics.mean(sizes)),measured=dict(live_bytes=live,physical_bytes=physical,live_bytes_per_row=live/1e7,physical_bytes_per_row=physical/1e7,cache_per_owner_maxima_sum_bytes=owned,cache_maxima_bytes_per_row=owned/1e7,selected_policy=policy),owner_details=rows,offline_fixed_tree_placements={str(hosts):dict(mean_rpc_batches=statistics.mean(values['rpc_batches']),mean_wire_candidates=statistics.mean(values['wire_candidates']),max_wire_candidates=max(values['wire_candidates']),mean_sequential_phases=statistics.mean(values['phase_count']),max_host_physical_bytes=max(mappings[hosts][1]),adapters_per_host=4) for hosts,values in totals.items()},arithmetic_100b=dict(rows=100000000000,dimension=768,raw_f32_bytes=100000000000*768*4,live_bytes_linear=live*10000,physical_bytes_linear=physical*10000,owned_posting_cache_bytes_linear=owned*10000,global_u64_map_bytes=100000000000*8,global_dense_audit_bitmap_bytes=math.ceil(100000000000/8)),limitations=['Storage and cache coefficient extrapolations are arithmetic scenarios, not measured100B requirements or upper bounds.','Private checkpoint amplification, routing quality, height, skew, and hot-generation count can change with scale.','Cache maxima are not simultaneous RSS; provider caches/process baseline/OS cache are additional.','Offline regrouping computes contribution counts using disjoint ownership and pre-reduction local counts; it does not measure new network latency or recompute candidate contents.','No250M or larger-prefix preparation/benchmark is authorized by this study.'],sources={str(path.relative_to(ROOT)):digest(path) for path in (storage_archive,input_archive,RESULTS/'fineweb10m-calibration-independent-review.json',RESULTS/'fineweb10m-selected-storage-summary-v1.json',RESULTS/'fineweb10m-read-immutable-metadata-v1.tar.gz',RESULTS/'load32-independent-review.json')})
    (Path(__file__).parent/'measured-model.json').write_text(json.dumps(model,indent=2)+'\n')
    print(json.dumps({key:value for key,value in model.items() if key not in ('owner_details','sources','measured')},indent=2))


if __name__=='__main__':main()
