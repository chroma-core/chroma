import copy,hashlib,importlib.util,json,math,os,struct,sys,tempfile,time,unittest
from pathlib import Path
from unittest.mock import patch
from harness_common import descriptor,write
import quality_analysis as a
from prepare_cohort import prepare
HERE=Path(__file__).resolve().parent
sp=importlib.util.spec_from_file_location('quality_run',HERE/'run-quality.py');m=importlib.util.module_from_spec(sp);sp.loader.exec_module(m)


def fake_reply(request,losses=False):
    pre=list(range(451));pre.remove(99 if losses else 450);kept=[i for i in pre if not losses or i!=98][:400];ids=list(range(97))+[100,101,102] if losses else list(range(100));gets=400
    block=dict(reader_requests=gets,pin_hits=gets,pin_misses=0,provider_obtains=0,provider_hits=0,provider_misses=0,storage_callbacks=0,encoded_bytes=0,decode_attempts=0,decode_successes=0,storage_callback_errors=0,fetch_pipeline_errors=0)
    levels=[dict(level=1,expanded_internal=1,offered_internal=2,offered_leaf=0,retained_internal=1,retained_leaf=0,discarded_internal=1,discarded_leaf=0),dict(level=2,expanded_internal=1,offered_internal=0,offered_leaf=1,retained_internal=0,retained_leaf=1,discarded_internal=0,discarded_leaf=0)]
    counts=dict(expanded_internal_nodes=2,centroid_distance_evaluations=3,navigation_levels=2,peak_child_frontier=2,selected_leaves=1,posting_rows_scored_before_dedup=450 if request['beam_min']==32 else 460,distinct_candidates_before_trim=450,candidates_after_trim=400,exact_embedding_gets=400,exact_vectors_reranked=400,nonempty_leaves_scored=1,navigation_level_trace=dict(complete=True,level_cap=128,levels=levels))
    return dict(request_id=request['request_id'],generation=1,shard_id='global',query_work_policy='ordinary-query-work-v1',query_navigation_policy='fp',provider_cache_policy=a.CACHE,rerank_factor=4,candidate_budget=400,query_navigation_snapshot=dict(generation=1,source_id='fake',dimension=768,metric='ip',root_ref=dict(kind='internal',id=9)),fp_navigation_cache=dict(nodes=5,dimension=768,metric='InnerProduct',root_id=9,owned_f32_payload_bytes=15360,logical_centroid_return_bytes=15360,vector_access=dict(access_overflow=False)),query_work=dict(counts=counts,returned_results=100,selected_leaves=1,returned_distance_bits=[0]*100),query_stage_trace_policy='bounded-query-stages-v1',query_stage_trace=dict(complete=True,id_cap=16384,selected_leaf_ids=[10],pretrim_candidate_ids=pre,retained_candidate_ids=kept),ids=ids,distances=[0.]*100,elapsed_ms=.01,query_reader_open_seconds=.001,vector_pin_observation=dict(access_overflow=True,point_blocks={str(i):'block' for i in range(256)},batch_point_blocks={},block_access={'block':block}))


def fake_snapshot(path):return dict(generation=1,source_id='fake',dimension=768,metric='ip',root_ref=dict(kind='internal',id=9),_store=str(path))
CENSUS=dict(reachable_nodes=5,reachable_leaves=2)


def fixture_preparation(out):
    prep=dict(queries={},ground_truth={})
    for split in ['calibration','evaluation']:
        chosen=[v for j,v in enumerate(a.cohort_ids()) if (j%9==0)==(split=='calibration')]
        queries=[dict(query_id='canonical-doc:'+str(i),canonical_global_id=i,vector=[.1]*768,split=split) for i in chosen];gt=[dict(query_id=q['query_id'],ids=list(range(100)),distances=[float(i) for i in range(100)]) for q in queries]
        for key,values in [('queries',queries),('ground_truth',gt)]:
            p=out/(split+'-'+key+'.jsonl');p.write_text(''.join(json.dumps(v)+'\n' for v in values));prep[key][split]=descriptor(p)
    write(out/'preparation.json',prep);return prep


class Quality(unittest.TestCase):
    def test_selection_and_tiny_fake_source_exact_gt(self):
        selection=json.loads((HERE/'cohort-selection.json').read_text());self.assertEqual(selection['ids'],a.cohort_ids());self.assertEqual((len(selection['calibration_ids']),len(selection['evaluation_ids'])),(32,256));self.assertTrue(set(selection['calibration_ids']).isdisjoint(selection['evaluation_ids']));self.assertTrue(min(selection['ids'])>=16384)
        with tempfile.TemporaryDirectory() as td:
            out=Path(td);source=out/'fake.f32';vectors=[(float(i),float(i*i+1)) for i in range(20)];source.write_bytes(b''.join(struct.pack('<2f',*v) for v in vectors));p=prepare(source,out,rows=8,dimension=2,source_rows=20,selected=[8,10,12,14,16,18,19,9,11,13],k=3)
            self.assertEqual(p['prefix_copy_bytes'],64);self.assertEqual(p['query_logical_read_bytes'],80)
            for split in ['calibration','evaluation']:
                qs=[json.loads(v) for v in Path(p['queries'][split]['path']).read_text().splitlines()];gt=[json.loads(v) for v in Path(p['ground_truth'][split]['path']).read_text().splitlines()]
                for q,v in zip(qs,gt):
                    expected=sorted((1-sum(x*y for x,y in zip(q['vector'],row)),i) for i,row in enumerate(vectors[:8]))[:3];self.assertEqual(v['ids'],[i for _,i in expected]);self.assertEqual(v['distances'],[d for d,_ in expected])
            for selected,mutate in [([8,8],False),([0,9],False),([8,9],True)]:
                target=out/('bad'+str(len(list(out.iterdir()))));target.mkdir()
                if mutate:
                    with source.open('r+b') as f:f.seek(8*8);f.write(struct.pack('<2f',*vectors[0]))
                with self.assertRaises(ValueError):prepare(source,target,rows=8,dimension=2,source_rows=20,selected=selected,k=3)

    def test_new_gt_stage_join_and_point_map_boundary(self):
        req=dict(request_id='new-query',beam_min=32);reply=fake_reply(req,True);gt=dict(query_id='new-query',ids=list(range(100)));value=a.validate_reply(reply,req,gt,fake_snapshot('unused'),CENSUS)
        self.assertEqual((value['unreached_current_candidate_loss'],value['trim_loss'],value['exact_loss']),(1,1,1));self.assertEqual(value['recall_at_100'],.97);self.assertTrue(value['vector_io']['available']);self.assertFalse(value['vector_io']['point_key_map_complete']);self.assertEqual(value['vector_io']['aggregate']['reader_requests'],400)
        bad=copy.deepcopy(reply);bad['query_stage_trace']['complete']=False
        with self.assertRaises(ValueError):a.validate_reply(bad,req,gt,fake_snapshot('unused'),CENSUS)
        obs=reply['vector_pin_observation'];bad=copy.deepcopy(obs);bad['block_access']={str(i):copy.deepcopy(next(iter(obs['block_access'].values()))) for i in range(256)};self.assertFalse(a.vector_traffic(bad,400)['available'])
        bad=copy.deepcopy(obs);bad['block_access']['block']['reader_requests']=399;self.assertFalse(a.vector_traffic(bad,400)['available'])
        bad=copy.deepcopy(obs);bad['point_blocks'].pop('0');self.assertFalse(a.vector_traffic(bad,400)['available'])
        bad=copy.deepcopy(reply);bad['query_navigation_snapshot']['root_ref']['id']=10
        with self.assertRaises(ValueError):a.validate_reply(bad,req,gt,fake_snapshot('unused'),CENSUS)

    def test_deterministic_inference_family_and_negative_outcomes(self):
        rng=a.SplitMix64(0);self.assertEqual(rng.next(),0xe220a8397b1dcdaf);self.assertEqual(rng.next(),0x6e789e6aa1b965f4);self.assertEqual(a.quantile([0.,1.],.005),.005)
        def evaluation(n=256,recall=1.,pruning=True):return {arm:[dict(query_id=str(i),recall_at_100=recall,structural=dict(internal_pruning=pruning)) for i in range(n)] for arm in a.ARMS}
        full=a.infer(evaluation());self.assertEqual(full['outcome'],'conditional-comparison-passed');self.assertEqual(full['pruning_threshold'],205);self.assertEqual(len(full['intervals']),5);self.assertTrue(all(v['coverage_each']==.99 for v in full['intervals'].values()))
        e=evaluation(8,.96);e['dynamic4'][0]['recall_at_100']=.92
        self.assertEqual(a.infer(e,resamples=256),a.infer(e,resamples=256));self.assertEqual(a.infer(e,resamples=256)['outcome'],'inconclusive')
        self.assertEqual(a.infer(evaluation(8,.94),resamples=128)['outcome'],'target-not-met');self.assertEqual(a.infer(evaluation(8,1.,False),resamples=128)['outcome'],'structurally-insufficient')
        e=evaluation(8);e['dynamic4']=[dict(v,recall_at_100=.96) for v in e['dynamic4']];self.assertEqual(a.infer(e,resamples=128)['outcome'],'quality-mismatch')

    def test_real_fake_subprocess_policy_lock_success_failure_and_tamper(self):
        # Fake native readers are actual bounded local subprocesses; no network,
        # corpus, AWS, watchdog or real accepted native binary is executed.
        for mode in ['success','calibration-negative','reader-error','tamper']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory() as td:
                out=Path(td);prep=fixture_preparation(out);fake=out/'fake-native';eval_ids=[v for j,v in enumerate(a.cohort_ids()) if j%9!=0];script='''#!PYTHON
import sys,json,os
from pathlib import Path
sys.path.insert(0,PACKAGE)
from test_quality import fake_reply
from harness_common import descriptor
for line in sys.stdin:
 request=json.loads(line);identity=int(request['request_id'].split(':')[-1]);evaluation=identity in EVAL_IDS
 if evaluation:
  path=Path(OUT)/'selection.json';lock=json.loads((Path(OUT)/'selection.lock.json').read_text());assert descriptor(path)==lock
  with (Path(OUT)/'evaluation-seen.jsonl').open('a') as f:f.write(json.dumps(lock)+'\\n')
  if MODE=='reader-error' and 'dynamic4' in sys.argv[sys.argv.index('--checkpoint-dir')+1]:print('injected native error',file=sys.stderr);raise SystemExit(7)
 reply=fake_reply(request)
 if MODE=='calibration-negative':reply['ids']=list(range(90))+list(range(100,110))
 print(json.dumps(reply))
'''.replace('PYTHON',sys.executable).replace('PACKAGE',repr(str(HERE))).replace('EVAL_IDS',repr(eval_ids)).replace('OUT',repr(str(out))).replace('MODE',repr(mode));fake.write_text(script);fake.chmod(0o755)
                snapshots={arm:fake_snapshot(out/arm) for arm in a.ARMS};censuses={arm:CENSUS for arm in a.ARMS};evaluated=[]
                def runner(arm,policy,split,snapshot,census,queries,gt,remaining):
                    value=m.cell(fake,arm,policy,split,snapshot,census,queries,gt,out,remaining,dict(os.environ))
                    if split=='evaluation':
                        evaluated.append(arm)
                        if mode=='tamper' and len(evaluated)==1:(out/'selection.json').write_text('{}')
                    return value
                with patch.object(m,'HERE',HERE):
                    if mode in ['reader-error','tamper']:
                        with self.assertRaises(ValueError):m.policy_cycle(out,prep,snapshots,censuses,runner,lambda cap,end:cap)
                        self.assertTrue((out/'selection.lock.json').exists());self.assertTrue((out/'evaluation-reference-beam32-result.json').exists())
                    else:
                        result=m.policy_cycle(out,prep,snapshots,censuses,runner,lambda cap,end:cap)
                        if mode=='success':self.assertEqual(result['outcome'],'evaluation-complete-awaiting-analysis');self.assertEqual(evaluated,list(a.ARMS));self.assertTrue(all(v['beam']==32 for v in json.loads((out/'selection.json').read_text())['decision']['chosen'].values()))
                        else:self.assertEqual(result['outcome'],'calibration-target-or-pruning-not-met');self.assertFalse((out/'evaluation-seen.jsonl').exists());self.assertFalse(list(out.glob('evaluation-*-requests.jsonl')))

if __name__=='__main__':unittest.main()
