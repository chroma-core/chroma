"""Fixed tiny construction/locality experiment; no lifecycle or retry authority."""
import argparse, hashlib, json, math, os, resource, subprocess, sys, time
from pathlib import Path
from harness_common import descriptor, require, verify, write

HERE=Path(__file__).resolve().parent
QUERY_SHA='59d9a8401439ee7ff255268bcd961280bbbed8d95446c4cbc89a385cdce80b57'
PREFIX_BYTES=8192*768*4

def check_queries(path, expected_ids, dimension=768):
    rows=[json.loads(line) for line in Path(path).read_text().splitlines()]
    require(len(rows)==16 and [x['query_id'] for x in rows]==expected_ids and len(set(expected_ids))==16,'frozen query order/cardinality')
    for row in rows:
        require(len(row['vector'])==dimension and all(type(v) in (int,float) and math.isfinite(v) for v in row['vector']),'finite query dimension')
        # Only compare IDs in a declared common namespace. Publisher query-row
        # offsets are not corpus IDs; their numeric overlap has no such meaning.
        if 'canonical_global_id' in row:
            require(type(row['canonical_global_id']) is int and row['canonical_global_id']>=8192,'query overlaps construction prefix')
        if 'global_id' in row and row.get('id_namespace')=='canonical-global':
            require(type(row['global_id']) is int and row['global_id']>=8192,'query overlaps construction prefix')
    return rows

def prepare(source, out, source_tree, expected_ids, rows=8192, dimension=768, k=100):
    """The production caller fixes8192x768; small tests exercise this helper."""
    started=time.monotonic();prefix_path=out/'evaluation-prefix.f32';count=rows*dimension*4
    h=hashlib.sha256();copied=0
    with Path(source).open('rb') as src,prefix_path.open('xb') as dst:
        while copied<count:
            chunk=src.read(min(1024*1024,count-copied));require(bool(chunk),'truncated prefix');dst.write(chunk);h.update(chunk);copied+=len(chunk)
        dst.flush();os.fsync(dst.fileno())
    copy_seconds=time.monotonic()-started
    query_path=HERE/'queries.jsonl';queries=check_queries(query_path,expected_ids,dimension)
    sys.path.insert(0,str(source_tree/'tools'))
    from hierarchical_sharding.exact_blocked import exact_blocked
    import numpy as np
    gt_start=time.monotonic();gt=exact_blocked(prefix_path,queries,dimension,k=k,metric='ip',corpus_block=8192,query_block=16)
    for row in gt:
        require(len(row['ids'])==k and len(set(row['ids']))==k and all(type(i) is int and 0<=i<rows for i in row['ids']),'exact GT extent/uniqueness')
        require(len(row['distances'])==k and all(math.isfinite(x) for x in row['distances']),'finite exact GT')
        require(list(zip(row['distances'],row['ids']))==sorted(zip(row['distances'],row['ids'])),'exact GT total order')
    gt_path=out/'prefix-gt.jsonl'
    with gt_path.open('x') as f:
        for row in gt:f.write(json.dumps(row)+'\n')
    result=dict(status='passed',prefix=descriptor(prefix_path),prefix_copy_bytes=copied,prefix_sha256=h.hexdigest(),prefix_copy_seconds=copy_seconds,ground_truth=descriptor(gt_path),queries=descriptor(query_path),gt_seconds=time.monotonic()-gt_start,setup_seconds=time.monotonic()-started,engine='existing exact_blocked float64 numpy BLAS',numpy_version=np.__version__,engine_source=descriptor(source_tree/'tools/hierarchical_sharding/exact_blocked.py'),rows=rows,dimension=dimension,k=k,query_ids=expected_ids,overlap_check='No canonical-global query IDs in frozen rows. Publisher row and test query IDs are not corpus identifiers; disjointness cannot be independently established from these artifacts. Existing test-labelled cohort is reused, not a fresh untouched holdout.',scope='Exact GT recomputed ONLY on dense evaluation prefix. One prefix copy is charged; no fullcorpus copy/filter2MGT.')
    write(out/'preparation.json',result);return result

def shape(x, owner):
    c=x['census'];require(c['unique_live_rows']==8192 and c['duplicate_current_entries']==c['unreachable_nodes']==0,'full saved census')
    require(len(x['phases'])==2 and [p['rows'] for p in x['phases']]==[4096,8192],'logical cuts')
    if owner:
        r=x['owner_report'];require(r['schedule_sample_overflow'] is False and sum(bool(v) for v in r['last_reference_subtree_assignments'])>=2 and r['peak_distinct_subtree_workers']>=2,'multi-region scheduler absent')
        require(r['placement_moves']>0 and sum(v['current']>0 for v in r['owners'])>=2,'movement absent')
        require(r['pending_balance']==r['central_current_versions']==r['central_mutable_posting_rows']==0,'unfinished/central mutation mirror')
        require(r['predicate_batching'] is True and r['predicate_batch_computed_rows']>0 and r['predicate_batch_callbacks']>0 and r['batched_scrub_validation'] is True and r['scrub_validation_rows']>0,'combined policies absent')
        require(r['direct_current_policy'] is False and r['direct_current_reads']==0,'direct pointer enabled')
        require(x['construction_policy']=='owner-batched-v1','policy echo')
    else:
        r=x['reference_shape'];require(r['nodes']==r['reachable_nodes'] and r['pending_balance']==0 and r['depth']>=2,'reference shape')

def checkpoint(x, native):
    pub=x['checkpoint_publication'];require(pub is not None and pub['generation']==1,'original checkpoint absent')
    path=Path(pub['manifest_path']);require(path==Path(native['output'])/'store/manifest.json','manifest output path')
    pinned=descriptor(path);require(pinned['sha256']==pub['manifest_sha256'] and pinned['bytes']==pub['manifest_bytes'],'original manifest hash')
    value=json.loads(path.read_text());require(value['generation']==1 and value['vector_count']==8192 and value['source_id']==native['source_id'] and value['metric']=='ip' and value['dimension']==768,'checkpoint metadata')
    require(value['block_ids']==x['roots'] and value['construction']['config']==native['config'],'saved roots/config')
    require(value['construction_input']['prefix_sha256']==native['prefix_sha256'] and value['construction_input']['vectors']==native['vectors'] and value['construction_input']['spec_sha256']==x['spec_sha256'],'checkpoint input binding')
    mapping=value['id_mapping'];require(mapping==pub['id_mapping'] and mapping['path']=='id-map-1.u64le' and mapping['count']==8192 and mapping['bytes']==65536,'dense map descriptor')
    map_path=path.parent/mapping['path'];mp=descriptor(map_path);require(mp['bytes']==mapping['bytes'] and mp['sha256']==mapping['sha256'],'persisted map binding')
    require(pub['map_write_bytes']==pub['map_verify_bytes']==65536,'map charge')
    return pinned,mp

def quality(report, manifest, prep, query_ids):
    require(report['policy']=='saved-gt-quality-v1' and report['declared_global_rows']==8192 and report['dimension']==768 and report['metric']=='ip','diagnostic metadata')
    require(report['ground_truth']==prep['ground_truth'] and report['query_vectors']==prep['queries'],'diagnostic inputs')
    require(report['raw_embedding_reads']==0 and len(report['owners'])==1,'diagnostic scope')
    owner=report['owners'][0];require(owner['owner_id']=='global' and owner['generation']==1 and owner['snapshot_sha256']==manifest['sha256'],'diagnostic checkpoint')
    require(owner['census']['reachable_leaves']<=4096,'saved leaf cap')
    gt=[json.loads(v) for v in Path(prep['ground_truth']['path']).read_text().splitlines()];union=set().union(*(set(v['ids']) for v in gt))
    require(report['gt_union_count']==len(union),'GT union differs');require([q['query_id'] for q in report['queries']]==query_ids,'diagnostic query order')
    require(all(q['expected']==q['covered']==100 and len(q['oracles'])==3 for q in report['queries']),'missing expected GT coverage')
    # The unchanged native diagnostic enforces single current copies and exact
    # disjoint leaf/weighted oracles. Retain its full leaf report for inspection.

def child(command, stem, out, remaining, env):
    require(remaining>0,'fixed deadline exhausted');start=time.monotonic();before=resource.getrusage(resource.RUSAGE_CHILDREN)
    try:
        with (out/(stem+'.stdout')).open('xb') as stdout,(out/(stem+'.stderr')).open('xb') as stderr:p=subprocess.run(command,stdout=stdout,stderr=stderr,env=env,timeout=remaining)
    except subprocess.TimeoutExpired:
        write(out/(stem+'-process.json'),dict(command=command,status='timeout',elapsed_seconds=time.monotonic()-start));raise
    after=resource.getrusage(resource.RUSAGE_CHILDREN);write(out/(stem+'-process.json'),dict(command=command,returncode=p.returncode,elapsed_seconds=time.monotonic()-start,user_cpu_seconds=after.ru_utime-before.ru_utime,system_cpu_seconds=after.ru_stime-before.ru_stime,voluntary_switches=after.ru_nvcsw-before.ru_nvcsw,involuntary_switches=after.ru_nivcsw-before.ru_nivcsw,scope='Whole child delta; no phase CPU attribution. Concurrent spans overlap.'))
    require(p.returncode==0,stem+' failed');return json.loads((out/(stem+'.stdout')).read_text())

def run(s):
    require(sys.platform=='linux','Linux runtime only');require(time.time()<s['deadline_unix'],'expired external deadline')
    require((s['rows'],s['dimension'],s['threads'],s['balance_every'],s['owner_slots'],s['move_above'])==(8192,768,4,4096,4,1024),'fixed cohort/configuration')
    require((s['setup_seconds'],s['arm_seconds'],s['total_seconds'],s['diagnostic_arm_seconds'],s['diagnostic_total_seconds'],s['scientific_seconds'])==(60,30,60,30,60,180),'fixed budgets')
    require(s['config']==json.loads((HERE/'runtime-spec.template.json').read_text())['config'],'common pinned FP config')
    require(descriptor(HERE/'queries.jsonl')['sha256']==QUERY_SHA,'frozen queries changed')
    provenance=json.loads((HERE/'query-provenance.json').read_text());require(s['query_ids']==provenance['query_ids'],'query selection changed')
    require(s['restore_receipt']==json.loads((HERE/'runtime-spec.template.json').read_text())['restore_receipt'],'fixed restore receipt descriptor');binary=verify(s['binary']);restore=json.loads(verify(s['restore_receipt']).read_text());require(restore['status']=='complete' and restore['canonical']==s['vectors'] and restore['rows']==2000000 and restore['dimension']==768,'restore binding')
    expected=json.loads((HERE/'runtime-spec.template.json').read_text())['vectors'];require(s['vectors']==expected,'fixed canonical descriptor');source=Path(s['vectors']['path']);require(source.stat().st_size==6144000000,'source extent')
    tree=Path(s['source']);manifest=json.loads((HERE/'source-manifest.json').read_text())
    # Compile helper already checks the full closure; recheck all exact source
    # files here before executing the pinned engine and native binary.
    from importlib.util import spec_from_file_location,module_from_spec
    sp=spec_from_file_location('compile_recipe',HERE/'compile-native.py');mod=module_from_spec(sp);sp.loader.exec_module(mod);mod.verify(tree,manifest)
    out=Path(s['output']);out.mkdir(exist_ok=False);started=time.monotonic();results={};diagnostics={};failure=None
    env=dict(os.environ,TOKIO_WORKER_THREADS='4',RAYON_NUM_THREADS='4',OPENBLAS_NUM_THREADS='1',OMP_NUM_THREADS='1',MKL_NUM_THREADS='1')
    def remaining(cap,phase_end):return min(cap,phase_end-time.monotonic(),s['deadline_unix']-time.time(),180-(time.monotonic()-started))
    try:
        setup_spec=out/'setup-spec.json';write(setup_spec,dict(source=str(source),source_tree=str(tree),output=str(out),query_ids=s['query_ids']))
        child([sys.executable,str(Path(__file__).resolve()),'--prepare',str(setup_spec)],'setup',out,remaining(60,started+60),env)
        prep=json.loads((out/'preparation.json').read_text());require(prep['prefix_copy_bytes']==PREFIX_BYTES and prep['rows']==8192 and prep['k']==100 and prep['queries']['sha256']==QUERY_SHA,'preparation binding')
        construction_end=time.monotonic()+60;bindings={}
        for arm in ['reference','owner']:
            native=dict(protocol='reference-owner-parallel-v1',source_id='retained-matched2m-first8192',vectors=s['vectors'],prefix_sha256=prep['prefix_sha256'],rows=8192,dimension=768,metric='ip',config=s['config'],treatment=arm,predicate_batching=arm=='owner',construction_observation=True,skip_placement=False,direct_current_reads=False,batched_scrub_validation=arm=='owner',publish_checkpoint=True,threads=4,balance_every=4096,owner_slots=4,move_above=1024,output=str(out/arm))
            path=out/(arm+'-spec.json');write(path,native);x=child([str(binary),'reference-owner-diagnostic','--spec',str(path)],arm,out,remaining(30,construction_end),env)
            require(x==json.loads((out/arm/'result.json').read_text()) and x['spec']==native and x['spec_sha256']==descriptor(path)['sha256'],'native output binding');shape(x,arm=='owner');bindings[arm]=checkpoint(x,native);results[arm]=x
        require(all(len(x['all_id_exact_fp_distance_bits'])==8192 for x in results.values()) and results['reference']['all_id_exact_fp_distance_bits']==results['owner']['all_id_exact_fp_distance_bits'],'saved exact distance bits differ')
        diagnosis_end=time.monotonic()+60
        for arm in ['reference','owner']:
            pinned,mp=bindings[arm];ds=dict(format_version=1,source_id='retained-matched2m-first8192',dimension=768,metric='ip',global_rows=8192,max_map_rows=8192,ground_truth=prep['ground_truth'],query_vectors=prep['queries'],selected_queries=[dict(query_id=q,expected=100) for q in s['query_ids']],owners=[dict(owner_id='global',generation=1,snapshot=pinned)])
            path=out/(arm+'-diagnostic-spec.json');write(path,ds);report=child([str(binary),'diagnose-ground-truth','--spec',str(path)],arm+'-diagnostic',out,remaining(30,diagnosis_end),env);require(report['spec_sha256']==descriptor(path)['sha256'] and report['source_id']==ds['source_id'],'diagnostic response spec/source binding');quality(report,pinned,prep,s['query_ids']);require(descriptor(pinned['path'])==pinned and descriptor(mp['path'])==mp,'saved manifest/map changed');write(out/(arm+'-quality.json'),report);diagnostics[arm]=dict(snapshot=pinned,map=mp,report=descriptor(out/(arm+'-quality.json')),map_accounting=dict(native_write_bytes=65536,native_verify_read_bytes=65536,controller_prepost_hash_read_bytes=131072,diagnostic_map_read_bytes=report['owners'][0]['map_bytes_read'],scope='Logical successful map payload reads/writes, not physicalIO; manifest/metadata reads separate.'))
    except Exception as e:failure=repr(e)
    finally:
        files=[p for p in out.rglob('*') if p.is_file()];stores=[p for p in files if any(p.is_relative_to(out/a/'store') for a in ['reference','owner'])];payload=[out/'evaluation-prefix.f32'];metadata=sum(p.stat().st_size for p in files if p not in stores and p not in payload)
        if metadata>50*1024**2:failure='metadata exceeds50MiB'
        result=dict(status='passed' if failure is None else 'failed',error=failure,elapsed_seconds=time.monotonic()-started,binary=s['binary'],results=results,diagnostics=diagnostics,metadata_bytes=metadata,retained_store_bytes=sum(p.stat().st_size for p in stores),evaluation_prefix_bytes=sum(p.stat().st_size for p in payload if p.exists()),scope='Locality only, not ANNrecall/searchlatency or physicalactorfanout. Independent stochastic construction, no causal speedup. Cachewarm/root/global/fullsnapshot costs retained. One separately charged prefix copy; unchanged engine FP64exactprefixGT. Query provenance reused test cohort, disjointness unproven without commonnamespace IDs. SourcefullSHA relies on accepted restore receipt; consumedprefixSHA verified. No retries/scalegrowth. Child timeout not detached descendant guard; external fixeddeadline/guard required.')
        write(out/'result.json',result)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--spec');p.add_argument('--prepare');a=p.parse_args()
    if a.prepare:
        require(sys.platform=='linux','Linux setup runtime only');s=json.loads(Path(a.prepare).read_text());v=prepare(s['source'],Path(s['output']),Path(s['source_tree']),s['query_ids']);print(json.dumps(v))
    else:
        require(a.spec is not None,'spec required');v=run(json.loads(Path(a.spec).read_text()));print(json.dumps(v));raise SystemExit(v['status']!='passed')
