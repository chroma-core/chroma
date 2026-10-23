"""Warm pilot curves. Separate calibration selects parameters; test stays held out."""
import argparse,json,pathlib,subprocess,sys,time
from tools.hierarchical_sharding.transport import query_all
p=argparse.ArgumentParser();p.add_argument('--manifest',required=True);p.add_argument('--queries',required=True);p.add_argument('--output',required=True);p.add_argument('--label',required=True);a=p.parse_args();out=pathlib.Path(a.output);out.mkdir(parents=True,exist_ok=True);data=pathlib.Path(a.queries);manifest=json.loads(pathlib.Path(a.manifest).read_text());cal=[json.loads(x) for x in (data/'calibration.jsonl').read_text().splitlines()];settings=[dict(beam_min=10,beam_max=b,rerank_factor=f) for b in (16,64,256) for f in (2,10,100)];summaries=[];calibration_started=time.monotonic();started_unix=time.time()
def run(label,pool,setting,concurrency=1,repetitions=1,seed=1729):
 prefix=out/label
 command=[sys.executable,'-m','tools.hierarchical_sharding.benchmark','run','--manifest',a.manifest,'--queries',str(data/(pool+'.jsonl')),'--reference',str(data/(pool+'.exact.jsonl')),'--output-prefix',str(prefix),'--k','100','--concurrency',str(concurrency),'--repetitions',str(repetitions),'--seed',str(seed),'--timeout','30','--regime','warm-after-calibration-query-pass','--query-settings',json.dumps(setting)]
 subprocess.run(command,check=True,stdout=subprocess.DEVNULL)
 return json.loads(pathlib.Path(str(prefix)+'.summary.json').read_text())
for n,setting in enumerate(settings):
 for q in cal:query_all(manifest,q['vector'],100,timeout=30,**setting)
 summary=run('calibration-'+str(n),'calibration',setting);summaries.append(dict(setting=setting,summary=summary))
selection={}
for target in (.9,.95):
 eligible=[r for r in summaries if r['summary']['mean_delivered_recall_at_k']>=target]
 selection[str(target)]=min(eligible,key=lambda r:r['summary']['latency_all_s']['p50'])['setting'] if eligible else None
(out/'calibration-selection.json').write_text(json.dumps(dict(label=a.label,started_unix=started_unix,calibration_elapsed_s=time.monotonic()-calibration_started,calibration_distinct_queries=len(cal),untimed_warmup_queries_per_setting=len(cal),fixed_read_tau=2.0,selection=selection,curves=summaries),indent=2)+'\n')
# Keep fixed-setting results even when a target cannot be met. Test each selected setting once per independent repetition.
fixed=dict(beam_min=10,beam_max=64,rerank_factor=10)
unique={json.dumps(fixed,sort_keys=True):fixed}
for setting in selection.values():
 if setting:unique[json.dumps(setting,sort_keys=True)]=setting
for n,setting in enumerate(unique.values()):
 for concurrency in (1,4):
  for repetition in range(3):run(f'test-setting-{n}-c{concurrency}-r{repetition}','test',setting,concurrency=concurrency,seed=1729+repetition)
