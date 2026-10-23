"""Frozen calibration/test curves and bounded concurrent load for one committed tree."""
import argparse
import concurrent.futures
import json
import multiprocessing
import pathlib
import statistics
import time
from .subtree_transport import TreeCoordinator


def rows(path):
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines()]


def compact(result):
    return {key:result[key] for key in ('ids','distances','global_candidate_count','actual_hosts','roundtrips','elapsed_s','levels','reached_leaves','selected_candidates','transport')}


def measure(tree,placement,queries,exact,width,budget,output):
    oracle={row['query_id']:set(row['ids'][:100]) for row in exact};records=[]
    coordinator=TreeCoordinator.from_files(tree,placement)
    try:
        coordinator.verify_roots()
        coordinator.query(queries[0]['vector'],100,beam_width=width,candidate_budget=budget)
        with pathlib.Path(output).open('w') as stream:
            for query in queries:
                result=coordinator.query(query['vector'],100,beam_width=width,candidate_budget=budget)
                record=dict(query_id=query['query_id'],recall=len(set(result['ids'])&oracle[query['query_id']])/100,result=compact(result))
                stream.write(json.dumps(record,allow_nan=False)+'\n');stream.flush();records.append(record)
    finally:coordinator.close()
    return dict(beam_width=width,candidate_budget=budget,queries=len(records),recall_at_100=statistics.mean(r['recall'] for r in records),mean_s=statistics.mean(r['result']['elapsed_s'] for r in records),median_s=statistics.median(r['result']['elapsed_s'] for r in records))


def curve(case,data,output):
    case=pathlib.Path(case);out=pathlib.Path(output);out.mkdir(exist_ok=False);tree=json.loads((case/'case.json').read_text())['tree'];data=pathlib.Path(data)
    calibration=rows(data/'calibration.jsonl');test=rows(data/'test.jsonl');cal_exact=rows(data/'calibration.exact.jsonl');test_exact=rows(data/'test.exact.jsonl')
    settings=[(width,1000) for width in (1,2,4,8,16,32,64)]
    calibrated=[]
    for width,budget in settings:
        r=measure(tree,case/'placement-two-hosts.json',calibration,cal_exact,width,budget,out/f'calibration-b{width}.jsonl');calibrated.append(r);print(json.dumps(dict(phase='calibration',**r)),flush=True)
    eligible=[r for r in calibrated if r['recall_at_100']>=.9]
    selected=min(eligible,key=lambda r:r['mean_s']) if eligible else max(calibrated,key=lambda r:r['recall_at_100'])
    (out/'selection.json').write_text(json.dumps(dict(rule='Lowest calibration mean latency with recall@100 >= 0.90; otherwise highest recall.',selected=selected,calibration=calibrated),indent=2))
    tested=[]
    for label in ('cohost','two-hosts'):
        r=measure(tree,case/('placement-'+label+'.json'),test,test_exact,selected['beam_width'],selected['candidate_budget'],out/f'test-{label}.jsonl');tested.append(dict(placement=label,**r));print(json.dumps(dict(phase='test',placement=label,**r)),flush=True)
    result=dict(tree=tree,policy='fixed-global-beam',cache='Persistent readers; one warmup query per configuration. OS page cache uncontrolled and warm after construction/copy.',selection=selected,test=tested,tail_latency='100 queries per pool; no supported p99 estimate.')
    (out/'summary.json').write_text(json.dumps(result,indent=2));return result


def load_process(args):
    tree,placement,queries,width,budget,start,duration,index,journal=args
    coordinator=TreeCoordinator.from_files(tree,placement);records=[]
    with pathlib.Path(journal).open('w') as stream:
        try:
            coordinator.verify_roots()
            while time.monotonic()<start:time.sleep(.005)
            counter=0
            while time.monotonic()<start+duration:
                query=queries[(index+counter)%len(queries)];began=time.monotonic()
                try:
                    result=coordinator.query(query['vector'],100,beam_width=width,candidate_budget=budget)
                    row=dict(status='ok',query_id=query['query_id'],elapsed_s=time.monotonic()-began,ids=result['ids'],actual_hosts=result['actual_hosts'],global_candidate_count=result['global_candidate_count'])
                except Exception as error:
                    stream.write(json.dumps(dict(status='error',query_id=query['query_id'],elapsed_s=time.monotonic()-began,error=str(error)))+'\n');stream.flush();raise
                stream.write(json.dumps(row)+'\n');stream.flush();records.append(row);counter+=1
        except Exception as error:
            stream.write(json.dumps(dict(status='client_failure',error=str(error)))+'\n');stream.flush();raise
        finally:coordinator.close()
    return records


def load(case,data,selection,output,duration=15,placements=("cohost","two-hosts"),concurrencies=(1,2,4,8,16)):
    case=pathlib.Path(case);out=pathlib.Path(output);out.mkdir(exist_ok=False);tree=json.loads((case/'case.json').read_text())['tree'];chosen=json.loads(pathlib.Path(selection).read_text())['selected'];queries=rows(pathlib.Path(data)/'test.jsonl');summary=[]
    # Separate processes avoid the Python coordinator's interpreter lock as client concurrency rises.
    for label in placements:
        for concurrency in concurrencies:
            start=time.monotonic()+10; started_unix=time.time()+10
            args=[(tree,str(case/('placement-'+label+'.json')),queries,chosen['beam_width'],chosen['candidate_budget'],start,duration,i,str(out/f'{label}-c{concurrency}-client-{i}.jsonl')) for i in range(concurrency)]
            failures=[];results=[]
            with concurrent.futures.ProcessPoolExecutor(max_workers=concurrency,mp_context=multiprocessing.get_context('spawn')) as pool:
                futures={pool.submit(load_process,arg):i for i,arg in enumerate(args)}
                for future in concurrent.futures.as_completed(futures):
                    try:results.append(future.result())
                    except Exception as error:failures.append(dict(client=futures[future],error=str(error)))
            if failures:
                (out/f'{label}-c{concurrency}-failure.json').write_text(json.dumps(dict(status='failed',failures=failures,completed_client_records=sum(len(v) for v in results),journals='Per-client JSONL files retain all completed queries and observed errors.'),indent=2))
                raise RuntimeError('Load phase failed; all client journals and completed outcomes are preserved.')
            elapsed=time.monotonic()-start;records=[r for part in results for r in part];r=dict(placement=label,started_unix=started_unix,finished_unix=time.time(),client_processes=concurrency,offered_window_s=duration,completion_window_s=elapsed,queries=len(records),qps=len(records)/elapsed,mean_s=statistics.mean(v['elapsed_s'] for v in records));summary.append(r)
            (out/f'{label}-c{concurrency}.json').write_text(json.dumps(dict(summary=r,records=records)));print(json.dumps(r),flush=True)
    (out/'summary.json').write_text(json.dumps(dict(settings=chosen,results=summary,scope='Closed loop clients; includes coordinator, HTTP queueing, traversal, reranking and cleanup. Driver saturation must be assessed separately.'),indent=2))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['curve','load']);p.add_argument('--case',required=True);p.add_argument('--data',required=True);p.add_argument('--output',required=True);p.add_argument('--selection');p.add_argument('--duration',type=float,default=15);a=p.parse_args()
    if a.mode=='curve':curve(a.case,a.data,a.output)
    else:load(a.case,a.data,a.selection,a.output,a.duration)

if __name__=='__main__':main()
