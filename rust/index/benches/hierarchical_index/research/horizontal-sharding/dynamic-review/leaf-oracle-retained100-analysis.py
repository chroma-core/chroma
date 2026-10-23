"""Read retained GT memberships and compute exact whole-leaf scan lower bounds."""
import hashlib,itertools,json,statistics
from pathlib import Path
BASE=Path(__file__).resolve().parents[1]
INPUT=BASE/'fleet/matched-pruning-16k-v1/runtime-evidence/runtime-attempt1'
OUTPUT=Path(__file__).with_suffix('.json')
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def minimum(items,target,weight):
    dp=[None]*(target+1);dp[0]=0
    for hits,live,physical in items:
        cost=(1,live,physical)[weight];new=dp.copy()
        for covered,old in enumerate(dp):
            if old is not None:
                nxt=min(target,covered+hits);value=old+cost
                if new[nxt] is None or value<new[nxt]:new[nxt]=value
        dp=new
    assert dp[target] is not None
    return dp[target]
def distribution(values):
    s=sorted(values)
    def q(p):
        at=(len(s)-1)*p;lo=int(at);return s[lo]+(s[min(lo+1,len(s)-1)]-s[lo])*(at-lo)
    return dict(count=len(s),mean=statistics.mean(s),minimum=s[0],p25=q(.25),median=q(.5),p75=q(.75),p95=q(.95),maximum=s[-1])
def main():
    gtfile=INPUT/'prefix-gt.jsonl';truth=[json.loads(x) for x in gtfile.read_text().splitlines()];gt={v['query_id']:set(v['ids']) for v in truth};bindings={str(gtfile):sha(gtfile)};arms={}
    for arm in ['reference','dynamic1','dynamic4']:
        records=[]
        for path in sorted(INPUT.glob(arm+'-diagnostic-*.stdout')):
            report=json.loads(path.read_text());assert sha(gtfile)==report['ground_truth']['sha256'];bindings[str(path)]=sha(path)
            leaves=[v for owner in report['owners'] for v in owner['census']['leaves']]
            memberships=[i for leaf in leaves for i in leaf['gt_ids']];assert len(memberships)==len(set(memberships))
            for query in report['queries']:
                qid=query['query_id'];expected=gt[qid];assert len(expected)==100
                items=[(len(expected.intersection(leaf['gt_ids'])),leaf['live_rows'],leaf['physical_rows']) for leaf in leaves];items=[v for v in items if v[0]];assert sum(v[0] for v in items)==100
                for old in query['oracles']:
                    assert minimum(items,old['target_hits'],0)==old['minimum_leaves'];assert minimum(items,old['target_hits'],1)==old['minimum_live_rows']
                row=dict(query_id=qid,minimum_leaves95=minimum(items,95,0),minimum_live_rows95=minimum(items,95,1),minimum_physical_rows95=minimum(items,95,2),gt_leaf_count=len(items));assert row['minimum_physical_rows95']>=row['minimum_live_rows95'];records.append(row)
        assert len(records)==100 and len({r['query_id'] for r in records})==100;arms[arm]=sorted(records,key=lambda r:r['query_id'])
    # Independent enumeration validates all three DP objectives, including cases
    # where minimizing leaf count and minimizing row cost choose different sets.
    example=[(6,100,105),(4,2,3),(3,3,4),(2,1,2)]
    for target in range(1,16):
        for weight in range(3):
            exact=min(sum((1,v[1],v[2])[weight] for v in subset) for n in range(5) for subset in itertools.combinations(example,n) if sum(v[0] for v in subset)>=target)
            assert exact==minimum(example,target,weight)
    metrics=['minimum_leaves95','minimum_live_rows95','minimum_physical_rows95'];summary={a:{m:distribution([r[m] for r in rows]) for m in metrics} for a,rows in arms.items()};pairs={}
    for a in ['dynamic1','dynamic4']:
        assert [r['query_id'] for r in arms[a]]==[r['query_id'] for r in arms['reference']]
        pairs[a+'_minus_reference']={}
        for m in metrics:
            ds=[x[m]-r[m] for x,r in zip(arms[a],arms['reference'])];pairs[a+'_minus_reference'][m]=dict(distribution=distribution(ds),lower_cost=sum(v<0 for v in ds),higher_cost=sum(v>0 for v in ds),equal_cost=sum(v==0 for v in ds))
    out=dict(status='completed-offline-retained-evidence',bindings=bindings,analysis_script_sha256=sha(Path(__file__)),cohort='The100reused natural queries from the original16K experiment; not32document calibration or256reserved evaluation.',target_hits=95,oracle_definition='Exact minimum sum of whole-leaf live or physical row weights over subsets containing at least95of100uniqueGT IDs. Minimum leaf count is a separate objective; each objective may choose different leaves.',validation=dict(retained_oracle_values_reproduced=1800,exhaustive_tiny_objective_target_checks=45,unique_complete_gt_memberships_per_query=True),summary=summary,paired_differences=pairs,per_query=arms,limitations=['Ground truth supplies an unattainable perfect leaf selector; internal navigation, centroid work, fetching and exact reranking costs are excluded.','Physical rows are logical entries, not blocks, encoded bytes, physical I/O or network. Row weights assume scanning whole selected leaves.','One stochastic tree per arm and reused queries provide descriptive conditional locality evidence; no equivalence, population quality, matched-recall latency or latest-construction conclusion.','Each saved snapshot is one global diagnostic owner; this does not measure construction actor fanout.','No new native/cloud/build/test suite executed. Only read-only arithmetic and a tiny exhaustive subset cross-check ran.'])
    OUTPUT.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(dict(summary=summary,paired_differences=pairs),indent=2));print('receipt',sha(OUTPUT))
if __name__=='__main__':main()
