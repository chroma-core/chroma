"""Fixed-tree, query-unit analysis. No population or between-build inference."""
import hashlib,json,math,statistics,struct
from navigation_work import validate_query_work_counts
from harness_common import require

ARMS=('reference','dynamic1','dynamic4')
POLICIES=({'name':'fp32-beam32-rerank4','beam':32},{'name':'fp32-beam36-rerank4','beam':36})
ROWS=16384
DIM=768
K=100
CACHE={'block_capacity_units':256,'root_capacity_entries':4096,'shards':1,'clear_after_operation':False}
BOOTSTRAP={'algorithm':'SplitMix64 with rejection-sampled indices; shared paired indices for all five statistics','seed_u64':2026101701,'resamples':10000,'quantile_method':'Sorted linear interpolation at (B-1)*p','family':'3 absolute recalls and 2 dynamic-minus-reference differences','interval_each':0.99,'family_approximate_coverage':0.95,'quantiles':[0.005,0.995]}

def cohort_ids():
    return [ROWS+j*(2000000-ROWS)//288 for j in range(288)]

def validate_gt(gt,query_ids,rows=ROWS,k=K):
    require(len(gt)==len(query_ids) and [v['query_id'] for v in gt]==query_ids,'GT order/count')
    for v in gt:
        ids=v['ids'];dist=v['distances'];require(len(ids)==len(set(ids))==k and all(type(i) is int and 0<=i<rows for i in ids),'GT IDs')
        require(len(dist)==k and all(type(z) in (int,float) and math.isfinite(z) for z in dist),'GT distances')
        require(list(zip(dist,ids))==sorted(zip(dist,ids)),'GT global-ID tie order')


def vector_traffic(obs,expected_gets):
    keys=['reader_requests','pin_hits','pin_misses','provider_obtains','provider_hits','provider_misses','storage_callbacks','encoded_bytes','decode_attempts','decode_successes']
    require(type(obs) is dict and type(obs['access_overflow']) is bool and type(obs['block_access']) is dict,'observer shape')
    blocks=obs['block_access'];require(len(blocks)<=256,'block observation capacity')
    for b in blocks.values():
        require(all(type(b[k]) is int and b[k]>=0 for k in keys),'block counter type')
        require(b['storage_callback_errors']==b['fetch_pipeline_errors']==0,'failed vector access')
    total={k:sum(v[k] for v in blocks.values()) for k in keys}
    complete=not obs['access_overflow'];point_complete=complete
    if obs['access_overflow']:
        # Frozen source only sets this flag for monotonic block or key-map caps.
        # A block map strictly below its cap cannot have dropped a new block.
        # Complete reader-request accounting closes the actual400-get boundary.
        complete=(len(blocks)<256 and len(obs['point_blocks'])==256 and not obs.get('batch_point_blocks') and total['reader_requests']==expected_gets)
        point_complete=False
    if not complete:return dict(available=False,point_key_map_complete=False,reason='Legacy overflow cannot be isolated to point-key mapping; no traffic claim.')
    require(total['reader_requests']==expected_gets and total['pin_hits']+total['pin_misses']==expected_gets,'vector requests partition')
    require(total['provider_obtains']==total['pin_misses'] and total['provider_hits']+total['provider_misses']==total['provider_obtains'],'provider partition')
    return dict(available=True,point_key_map_complete=point_complete,legacy_point_key_overflow=obs['access_overflow'],aggregate=total,scope='Aggregate reader/provider/storage-origin counters only. Truncated per-ID point map is never called complete. Encoded returns are not physical IO/network.')

def validate_reply(reply,request,gt,snapshot,census):
    require('error' not in reply and reply['request_id']==request['request_id'] and reply['generation']==snapshot['generation'] and reply['shard_id']=='global','reply identity')
    require(reply['query_work_policy']=='ordinary-query-work-v1' and reply['query_navigation_policy']=='fp' and reply['provider_cache_policy']==CACHE,'query policy')
    require(reply['rerank_factor']==4 and reply['candidate_budget']==400,'fixed rerank policy')
    identity={k:snapshot[k] for k in ['generation','source_id','dimension','metric','root_ref']};require(reply['query_navigation_snapshot']==identity,'saved generation binding')
    cache=reply['fp_navigation_cache'];require(cache['nodes']==census['reachable_nodes'] and cache['nodes']<=256 and cache['dimension']==DIM and cache['metric']=='InnerProduct' and cache['root_id']==snapshot['root_ref']['id'],'FP cache identity/cap')
    require(cache['owned_f32_payload_bytes']==cache['nodes']*DIM*4 and cache['logical_centroid_return_bytes']==cache['nodes']*DIM*4 and cache['vector_access']['access_overflow'] is False,'FP preload completeness')
    w=reply['query_work'];summary=validate_query_work_counts(w['counts'],saved_internal_nodes=census['reachable_nodes']-census['reachable_leaves'],require_trace=True);c=summary['scalar_counts']
    trace=reply['query_stage_trace'];require(reply['query_stage_trace_policy']=='bounded-query-stages-v1' and trace['complete'] is True and trace['id_cap']==ROWS,'complete stage trace')
    leaves=trace['selected_leaf_ids'];pre=trace['pretrim_candidate_ids'];kept=trace['retained_candidate_ids'];ids=reply['ids'];dist=reply['distances']
    for name,values in [('leaves',leaves),('pre',pre),('kept',kept),('returned',ids)]:
        require(type(values) is list and len(values)==len(set(values)) and len(values)<=ROWS and all(type(i) is int and i>=0 for i in values),'stage ID type/uniqueness: '+name)
    require(all(i<ROWS for i in pre+kept+ids),'dense map range');require(set(ids)<=set(kept)<=set(pre),'stage subset')
    require([len(leaves),len(pre),len(kept)]==[c['selected_leaves'],c['distinct_candidates_before_trim'],c['candidates_after_trim']],'stage count')
    require(c['posting_rows_scored_before_dedup']>=len(pre) and len(kept)==min(len(pre),400),'raw scoring/trim')
    require(c['exact_embedding_gets']==c['exact_vectors_reranked']==len(kept) and w['returned_results']==len(ids)==min(100,len(kept)),'exact reranks/returns')
    require(len(dist)==len(ids) and all(type(v) in (int,float) and math.isfinite(v) for v in dist),'returned finite distances')
    require(w['returned_distance_bits']==[struct.unpack('<I',struct.pack('<f',v))[0] for v in dist],'FP32 distance bits')
    require(w['selected_leaves']==len(leaves) and c['nonempty_leaves_scored']<=len(leaves),'leaf counters')
    truth=set(gt['ids']);hits=[len(set(v)&truth) for v in [pre,kept,ids]];require(hits[0]>=hits[1]>=hits[2],'GT stage monotonicity')
    io=vector_traffic(reply['vector_pin_observation'],c['exact_embedding_gets'])
    return dict(query_id=gt['query_id'],recall_at_100=hits[2]/100,reached_current_candidate_gt_hits=hits[0],retained_gt_hits=hits[1],returned_gt_hits=hits[2],unreached_current_candidate_loss=100-hits[0],trim_loss=hits[0]-hits[1],exact_loss=hits[1]-hits[2],structural=summary,counts=c,elapsed_ms=reply['elapsed_ms'],vector_io=io,returned_ids=ids,returned_distance_bits=w['returned_distance_bits'],coverage_scope='New GT joined directly to complete pretrim current-candidate IDs. This is not an independent selected-leaf membership census.')


def summarize(rows):
    return dict(queries=len(rows),mean_recall=statistics.mean(v['recall_at_100'] for v in rows),pruning_queries=sum(v['structural']['internal_pruning'] for v in rows),mean_posting_rows=statistics.mean(v['counts']['posting_rows_scored_before_dedup'] for v in rows),mean_centroid_evaluations=statistics.mean(v['counts']['centroid_distance_evaluations'] for v in rows),median_trace_warm_ms=statistics.median(v['elapsed_ms'] for v in rows))


def choose(calibration):
    require(set(calibration)==set(ARMS),'calibration arms');chosen={};failures={}
    for arm in ARMS:
        require(len(calibration[arm])==2,'two fixed policies');eligible=[]
        for policy,value in zip(POLICIES,calibration[arm]):
            require(value['policy']==policy and len(value['measured'])==32,'calibration policy/count')
            qids=[v['query_id'] for v in value['measured']];require(len(set(qids))==32,'calibration query duplicates');s=summarize(value['measured'])
            if s['mean_recall']>=.95 and s['pruning_queries']>=26:eligible.append((s['mean_posting_rows'],s['mean_centroid_evaluations'],len(eligible),policy))
        if eligible:chosen[arm]=min(eligible,key=lambda v:(v[0],v[1],v[3]['beam']))[3]
        else:failures[arm]='Neither prespecified policy reaches sample mean95% and26/32 actual pruning. No evaluation or target change.'
    return dict(chosen=chosen,failures=failures,eligible_for_evaluation=not failures,rule='Minimum mean raw posting rows, then mean centroid evaluations, then fixed catalog order among eligible calibration points.')


class SplitMix64:
    def __init__(self,seed):self.state=seed
    def next(self):
        mask=(1<<64)-1;self.state=(self.state+0x9E3779B97F4A7C15)&mask;z=self.state;z=((z^(z>>30))*0xBF58476D1CE4E5B9)&mask;z=((z^(z>>27))*0x94D049BB133111EB)&mask;return z^(z>>31)
    def index(self,n):
        limit=(1<<64)-((1<<64)%n)
        while True:
            v=self.next()
            if v<limit:return v%n


def quantile(sorted_values,p):
    rank=(len(sorted_values)-1)*p;lo=math.floor(rank);hi=math.ceil(rank);return sorted_values[lo]+(sorted_values[hi]-sorted_values[lo])*(rank-lo)


def infer(evaluation,resamples=10000):
    require(set(evaluation)==set(ARMS),'evaluation arms');ids=[v['query_id'] for v in evaluation['reference']];n=len(ids);require(n>0 and len(set(ids))==n,'evaluation queries')
    require(all([v['query_id'] for v in evaluation[a]]==ids for a in ARMS),'paired evaluation order')
    streams={a:[v['recall_at_100'] for v in evaluation[a]] for a in ARMS};require(all(all(type(z) in (int,float) and math.isfinite(z) and 0<=z<=1 for z in vals) for vals in streams.values()),'recall range')
    for a in ARMS[1:]:streams[a+'-reference']=[v-w for v,w in zip(streams[a],streams['reference'])]
    names=list(streams);data=[streams[k] for k in names];boot=[[] for _ in names];rng=SplitMix64(BOOTSTRAP['seed_u64'])
    for _ in range(resamples):
        totals=[0.]*5
        for _ in range(n):
            i=rng.index(n)
            for k in range(5):totals[k]+=data[k][i]
        for k in range(5):boot[k].append(totals[k]/n)
    intervals={}
    for name,values in zip(names,boot):
        values.sort();intervals[name]=dict(mean=statistics.mean(streams[name]),lower=quantile(values,.005),upper=quantile(values,.995),coverage_each=.99)
    pruning={a:sum(bool(v['structural']['internal_pruning']) for v in evaluation[a]) for a in ARMS};threshold=math.ceil(.8*n)
    if any(v<threshold for v in pruning.values()):outcome='structurally-insufficient'
    elif any(intervals[a]['mean']<.95 for a in ARMS):outcome='target-not-met'
    elif any(intervals[a]['lower']>.01 or intervals[a]['upper']<-.01 for a in names[3:]):outcome='quality-mismatch'
    elif all(intervals[a]['lower']>=.95 for a in ARMS) and all(intervals[a]['lower']>=-.01 and intervals[a]['upper']<=.01 for a in names[3:]):outcome='conditional-comparison-passed'
    else:outcome='inconclusive'
    return dict(outcome=outcome,queries=n,pruning_queries=pruning,pruning_threshold=threshold,intervals=intervals,bootstrap=dict(BOOTSTRAP,resamples=resamples),scope='Conditional fixed-tree/cohort query-unit resampling. Approximate Bonferroni five-interval family; deterministic correlated documents do not support population certification or between-build/algorithm equivalence. No evaluation retuning or sample extension.')
