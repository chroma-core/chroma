"""Frozen calibration/test curves and bounded concurrent load for one committed tree."""
import argparse
import hashlib
import concurrent.futures
import json
import multiprocessing
import os
import pathlib
import statistics
import time
from .subtree_transport import TreeCoordinator


def rows(path):
    return [json.loads(line) for line in pathlib.Path(path).read_text().splitlines()]


def compact(result):
    row={key:result[key] for key in ('ids','distances','global_candidate_count','actual_hosts','roundtrips','elapsed_s','levels','reached_leaves','selected_candidates','transport')}
    row['candidate_contributions']=result.get('candidate_contributions')
    row['wire_candidate_contributions']=result.get('wire_candidate_contributions')
    row['worker_metrics']=[dict(subtree_id=op['subtree_id'],op=op['op'],metrics={key:value for key,value in op['result'].items() if key not in ('children','candidates','results')}) for op in result.get('worker_operations',[])]
    return row


def measure(tree,placement,queries,exact,width,budget,output):
    oracle={row['query_id']:set(row['ids'][:100]) for row in exact};records=[];failure=None;started=time.time()
    coordinator=TreeCoordinator.from_files(tree,placement);deadline=time.monotonic()+600
    with pathlib.Path(output).open('w') as stream:
        try:
            coordinator.verify_roots()
            coordinator.query(queries[0]['vector'],100,beam_width=width,candidate_budget=budget,timeout=min(30,max(.001,deadline-time.monotonic())))
            for query in queries:
                if time.monotonic()>=deadline:
                    failure=dict(status='setting_deadline',error='Configuration exceeds the declared 600-second wall limit',completed_queries=len(records));stream.write(json.dumps(failure)+'\n');stream.flush();break
                began=time.time()
                try:
                    result=coordinator.query(query['vector'],100,beam_width=width,candidate_budget=budget,timeout=min(30,max(.001,deadline-time.monotonic())))
                except Exception as error:
                    failure=dict(status='error',query_id=query['query_id'],started_unix=began,finished_unix=time.time(),error=str(error));stream.write(json.dumps(failure)+'\n');stream.flush();break
                record=dict(status='ok',started_unix=began,finished_unix=time.time(),query_id=query['query_id'],recall=len(set(result['ids'])&oracle[query['query_id']])/100,result=compact(result))
                stream.write(json.dumps(record,allow_nan=False)+'\n');stream.flush();records.append(record)
        except Exception as error:
            failure=dict(status='setup_or_warmup_error',error=str(error));stream.write(json.dumps(failure)+'\n');stream.flush()
        finally:coordinator.close()
    summary=dict(status='failed' if failure else 'passed',beam_width=width,candidate_budget=budget,queries=len(records),expected_queries=len(queries),started_unix=started,finished_unix=time.time(),failure=failure)
    if not failure:summary.update(recall_at_100=statistics.mean(r['recall'] for r in records),mean_s=statistics.mean(r['result']['elapsed_s'] for r in records),median_s=statistics.median(r['result']['elapsed_s'] for r in records))
    return summary


def choose_setting(calibrated,target):
    calibrated=[row for row in calibrated if row.get('status','passed')=='passed']
    if not calibrated:return None
    eligible=[row for row in calibrated if row['recall_at_100']>=target]
    if eligible:return min(eligible,key=lambda row:(row['mean_s'],row['beam_width'],row['candidate_budget']))
    return min(calibrated,key=lambda row:(-row['recall_at_100'],row['mean_s'],row['beam_width'],row['candidate_budget']))


def curve(case,data,output,settings=None,target=.9,calibration_only=False):
    case=pathlib.Path(case);out=pathlib.Path(output);out.mkdir(exist_ok=False);tree=json.loads((case/'case.json').read_text())['tree'];data=pathlib.Path(data)
    settings=settings or [(width,1000) for width in (1,2,4,8,16,32,64)]
    if not 0<target<=1:raise ValueError('quality target must be within (0,1]')
    def sha(path):
        with pathlib.Path(path).open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()
    metadata=json.loads((case/'case.json').read_text())
    read_epoch=json.loads((case/'read-epoch.json').read_text()) if (case/'read-epoch.json').exists() else {}
    metadata['binary']=read_epoch.get('binary',metadata['binary'])
    plan=dict(tree=tree,tree_sha256=metadata['tree_sha256'],binary=metadata['binary'],binary_sha256=sha(metadata['binary']),read_epoch=read_epoch,per_setting_wall_limit_s=600,per_query_timeout_s=30,primary_recall_target=target,secondary_recall_target=.9,grid=[dict(beam_width=width,candidate_budget=budget) for width,budget in settings],selection_rule='Lowest calibration mean latency meeting the primary recall target; otherwise highest recall, explicitly marked as a fallback.',input_sha256={name:sha(data/name) for name in ('calibration.jsonl','calibration.exact.jsonl','test.jsonl','test.exact.jsonl')})
    (out/'plan.json').write_text(json.dumps(plan,indent=2))
    calibration=rows(data/'calibration.jsonl');cal_exact=rows(data/'calibration.exact.jsonl')
    calibrated=[]
    for width,budget in settings:
        r=measure(tree,case/'placement-two-hosts.json',calibration,cal_exact,width,budget,out/f'calibration-b{width}-m{budget}.jsonl');calibrated.append(r);print(json.dumps(dict(phase='calibration',**r)),flush=True)
    selected=choose_setting(calibrated,target)
    if selected is None:
        result=dict(status='all_calibration_settings_failed',plan=plan,selected=None,calibration=calibrated,primary_target_met=False)
        (out/'selection.json').write_text(json.dumps(result,indent=2));(out/'summary.json').write_text(json.dumps(result,indent=2));return result
    primary_target_met=selected['recall_at_100']>=target
    (out/'selection.json').write_text(json.dumps(dict(plan=plan,primary_target_met=primary_target_met,fallback=not primary_target_met,selected=selected,calibration=calibrated),indent=2))
    if calibration_only:
        result=dict(status='calibration_complete',selection=selected,primary_target_met=primary_target_met,calibration=calibrated)
        (out/'summary.json').write_text(json.dumps(result,indent=2));return result
    # The held-out pool is opened only after the immutable selection is saved.
    test=rows(data/'test.jsonl');test_exact=rows(data/'test.exact.jsonl')
    tested=[]
    for label in ('cohost','two-hosts'):
        r=measure(tree,case/('placement-'+label+'.json'),test,test_exact,selected['beam_width'],selected['candidate_budget'],out/f'test-{label}.jsonl');tested.append(dict(placement=label,**r));print(json.dumps(dict(phase='test',placement=label,**r)),flush=True)
    result=dict(tree=tree,policy='fixed-global-beam',cache='Persistent readers; one warmup query per configuration. OS page cache uncontrolled and warm after construction/copy.',selection=selected,primary_target=target,primary_target_met_on_calibration=primary_target_met,test=tested,tail_latency='100 queries per pool; no supported p99 estimate.')
    (out/'summary.json').write_text(json.dumps(result,indent=2));return result


def load_process(args):
    tree,placement,queries,width,budget,start,duration,index,journal=args[:9]
    barrier=args[9:] if len(args)>9 else None
    coordinator=TreeCoordinator.from_files(tree,placement);records=[]
    with pathlib.Path(journal).open('w') as stream:
        try:
            coordinator.verify_roots()
            if barrier:
                ready,go,shared_start,abort=barrier
                stream.write(json.dumps(dict(status='ready',client=index,pid=os.getpid(),unix=time.time()))+'\n');stream.flush();ready.put(dict(client=index,pid=os.getpid(),status='ready'))
                if not go.wait(90):raise TimeoutError('load readiness barrier timed out')
                if abort.value:raise RuntimeError('load readiness barrier aborted')
                start=shared_start.value
            while time.monotonic()<start:time.sleep(.005)
            counter=0
            while time.monotonic()<start+duration:
                query=queries[(index+counter)%len(queries)];began=time.monotonic()
                try:
                    result=coordinator.query(query['vector'],100,beam_width=width,candidate_budget=budget)
                    row=dict(status='ok',query_id=query['query_id'],started_unix=time.time()-(time.monotonic()-began),finished_unix=time.time(),finished_monotonic=time.monotonic(),elapsed_s=time.monotonic()-began,ids=result['ids'],actual_hosts=result['actual_hosts'],global_candidate_count=result['global_candidate_count'],candidate_contributions=result.get('candidate_contributions'),wire_candidate_contributions=result.get('wire_candidate_contributions'),transport=result.get('transport',[]))
                    row['worker_metrics']=[dict(subtree_id=op['subtree_id'],op=op['op'],metrics={key:op['result'][key] for key in ('elapsed_ms','adapter_timing','retained_postings') if key in op['result']}) for op in result.get('worker_operations',[])]
                    if 'levels' in result:
                        signature={key:result[key] for key in ('ids','distances','levels','reached_leaves','selected_candidates','global_candidate_count','candidate_contributions')}
                        row['trace_sha256']=hashlib.sha256(json.dumps(signature,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
                except Exception as error:
                    stream.write(json.dumps(dict(status='error',query_id=query['query_id'],started_unix=time.time()-(time.monotonic()-began),finished_unix=time.time(),elapsed_s=time.monotonic()-began,error=str(error)))+'\n');stream.flush();raise
                stream.write(json.dumps(row)+'\n');stream.flush();records.append(row);counter+=1
        except Exception as error:
            if barrier:barrier[0].put(dict(client=index,status='failed',error=str(error)))
            stream.write(json.dumps(dict(status='client_failure',error=str(error)))+'\n');stream.flush();raise
        finally:coordinator.close()
    return records


def journal_outcomes(paths):
    counts=dict(ok=0,error=0,client_failure=0,ready=0,invalid=0);successful=[]
    for path in paths:
        if not pathlib.Path(path).exists():counts['invalid']+=1;continue
        for line in pathlib.Path(path).read_text().splitlines():
            try:row=json.loads(line);status=row['status']
            except (ValueError,KeyError):counts['invalid']+=1;continue
            if status not in counts:counts['invalid']+=1;continue
            counts[status]+=1
            if status=='ok':successful.append(row)
    return counts,successful


def load(case,data,selection,output,duration=15,placements=("cohost","two-hosts"),concurrencies=(1,2,4,8,16)):
    case=pathlib.Path(case);out=pathlib.Path(output);out.mkdir(exist_ok=False);tree=json.loads((case/'case.json').read_text())['tree'];chosen=json.loads(pathlib.Path(selection).read_text())['selected'];queries=rows(pathlib.Path(data)/'test.jsonl');summary=[]
    # Separate processes avoid the Python coordinator's interpreter lock as client concurrency rises.
    for label in placements:
        for concurrency in concurrencies:
            failures=[];results=[];readiness=[]
            with multiprocessing.get_context('spawn').Manager() as manager:
                ready=manager.Queue();go=manager.Event();shared_start=manager.Value('d',0.0);abort=manager.Value('b',False)
                args=[(tree,str(case/('placement-'+label+'.json')),queries,chosen['beam_width'],chosen['candidate_budget'],None,duration,i,str(out/f'{label}-c{concurrency}-client-{i}.jsonl'),ready,go,shared_start,abort) for i in range(concurrency)]
                with concurrent.futures.ProcessPoolExecutor(max_workers=concurrency,mp_context=multiprocessing.get_context('spawn')) as pool:
                    futures={pool.submit(load_process,arg):i for i,arg in enumerate(args)}
                    try:
                        deadline=time.monotonic()+90
                        for _ in range(concurrency):
                            row=ready.get(timeout=max(.001,deadline-time.monotonic()));readiness.append(row)
                            if row['status']!='ready':raise RuntimeError('client failed before load window')
                    except Exception as error:
                        abort.value=True;failures.append(dict(phase='readiness',error=str(error)))
                    start=time.monotonic()+1;started_unix=time.time()+1;shared_start.value=start;go.set()
                    for future in concurrent.futures.as_completed(futures):
                        try:results.append(future.result())
                        except Exception as error:failures.append(dict(client=futures[future],error=str(error)))
            (out/f'{label}-c{concurrency}-readiness.json').write_text(json.dumps(dict(clients=readiness,started_unix=started_unix,all_ready=len(readiness)==concurrency and all(r['status']=='ready' for r in readiness)),indent=2))
            outcome_counts,all_successes=journal_outcomes([out/f'{label}-c{concurrency}-client-{i}.jsonl' for i in range(concurrency)])
            if outcome_counts['error'] or outcome_counts['client_failure'] or outcome_counts['invalid']:
                if not failures:failures.append(dict(phase='journal_validation',error='Observed failed or invalid outcomes'))
            if failures:
                (out/f'{label}-c{concurrency}-failure.json').write_text(json.dumps(dict(status='failed',failures=failures,completed_client_records=outcome_counts['ok'],outcome_counts=outcome_counts,journals='Per-client JSONL files retain all completed queries and observed errors.'),indent=2))
                raise RuntimeError('Load phase failed; all client journals and completed outcomes are preserved.')
            records=[r for part in results for r in part]
            if len(records)!=outcome_counts['ok']:raise RuntimeError('Load journals differ from completed outcomes')
            elapsed=max(duration,max(r['finished_monotonic'] for r in records)-start);r=dict(placement=label,started_unix=started_unix,finished_unix=time.time(),client_processes=concurrency,offered_window_s=duration,completion_window_s=elapsed,harness_finished_unix=time.time(),outcome_counts=outcome_counts,queries=len(records),qps=len(records)/elapsed,mean_s=statistics.mean(v['elapsed_s'] for v in records));summary.append(r)
            (out/f'{label}-c{concurrency}.json').write_text(json.dumps(dict(summary=r,records=records)));print(json.dumps(r),flush=True)
    (out/'summary.json').write_text(json.dumps(dict(settings=chosen,results=summary,scope='Closed loop clients; includes coordinator, HTTP queueing, traversal, reranking and cleanup. Driver saturation must be assessed separately.'),indent=2))


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('mode',choices=['curve','load']);p.add_argument('--case',required=True);p.add_argument('--data',required=True);p.add_argument('--output',required=True);p.add_argument('--selection');p.add_argument('--duration',type=float,default=15);p.add_argument('--beam-widths',nargs='+',type=int);p.add_argument('--candidate-budgets',nargs='+',type=int,default=[1000]);p.add_argument('--quality-target',type=float,default=.9);a=p.parse_args()
    if a.mode=='curve':curve(a.case,a.data,a.output,[(width,budget) for width in a.beam_widths for budget in a.candidate_budgets] if a.beam_widths else None,a.quality_target)
    else:load(a.case,a.data,a.selection,a.output,a.duration)

if __name__=='__main__':main()
