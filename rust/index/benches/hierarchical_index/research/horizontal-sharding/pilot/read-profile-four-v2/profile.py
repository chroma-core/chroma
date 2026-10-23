"""Isolated attribution run; all requests are journaled before aggregation."""
import collections, concurrent.futures, hashlib, json, multiprocessing, pathlib, statistics, sys, time
sys.path.insert(0, '/mnt/spann/results/read-profile-v2')
from tools.hierarchical_sharding.subtree_transport import TreeCoordinator
CASE=pathlib.Path('/mnt/spann/results/target-fineweb100k-audited-v3')
OUT=pathlib.Path('/mnt/spann/results/read-profile-four-v2')
DATA=pathlib.Path('/mnt/spann/fineweb-pilot-preparation-v1/queries')
def run(arg):
    client,start,duration,concurrency=arg
    queries=[json.loads(s) for s in (DATA/'test.jsonl').read_text().splitlines()]
    coordinator=TreeCoordinator.from_files(CASE/'tree-1.json',OUT/'placement.json')
    records=[]
    with (OUT/f'c{concurrency}-client{client}.jsonl').open('w') as stream:
        try:
            coordinator.verify_roots()
            coordinator.query(queries[client]['vector'],100,beam_width=64,candidate_budget=1000)
            while time.monotonic()<start:time.sleep(.005)
            counter=0
            while time.monotonic()<start+duration:
                query=queries[(client+counter)%len(queries)];began=time.monotonic()
                try:
                    result=coordinator.query(query['vector'],100,beam_width=64,candidate_budget=1000)
                    signature={k:result[k] for k in ['ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count']}
                    row=dict(status='ok',query_id=query['query_id'],elapsed_s=time.monotonic()-began,
                             trace_sha256=hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
                             ids=result['ids'],operations=[dict(subtree_id=o['subtree_id'],op=o['op'],elapsed_ms=o['result']['elapsed_ms'],
                                 memory_bytes=o['result']['memory_bytes'],adapter_timing=o['result']['adapter_timing'],
                                 candidates=o['result'].get('actual_candidate_count'),exact_vectors=o['result'].get('exact_vectors_scored')) for o in result['worker_operations']],transport=result['transport'])
                except Exception as error:
                    row=dict(status='error',query_id=query['query_id'],elapsed_s=time.monotonic()-began,error=str(error))
                stream.write(json.dumps(row,allow_nan=False)+'\n');stream.flush();records.append(row);counter+=1
        except Exception as error:
            row=dict(status='client_failure',error=str(error));stream.write(json.dumps(row)+'\n');stream.flush();records.append(row)
        finally:coordinator.close()
    return records
if __name__=='__main__':
    concurrency=int(sys.argv[1]);duration=15;start=time.monotonic()+8;started_unix=time.time()+8
    with concurrent.futures.ProcessPoolExecutor(max_workers=concurrency,mp_context=multiprocessing.get_context('spawn')) as pool:
        records=[row for part in pool.map(run,[(i,start,duration,concurrency) for i in range(concurrency)]) for row in part]
    elapsed=time.monotonic()-start;ok=[r for r in records if r['status']=='ok'];ops=collections.defaultdict(list)
    for r in ok:
        for op in r['operations']:ops[op['op']].append(op)
    result=dict(started_unix=started_unix,finished_unix=time.time(),concurrency=concurrency,offered_window_s=duration,completion_window_s=elapsed,successes=len(ok),failures=len(records)-len(ok),qps=len(ok)/elapsed,mean_s=statistics.mean(r['elapsed_s'] for r in ok) if ok else None,operations={})
    for name,rows in ops.items():
        result['operations'][name]=dict(count=len(rows),rust_ms=statistics.mean(r['elapsed_ms'] for r in rows),**{key:statistics.mean(r['adapter_timing'][key] for r in rows) for key in rows[0]['adapter_timing']})
    (OUT/f'c{concurrency}-summary.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)
