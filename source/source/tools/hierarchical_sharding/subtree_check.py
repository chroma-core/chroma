"""Compare one committed tree through direct, one-host, and two-host execution."""
import argparse
import json
import math
from pathlib import Path
from .subtree_transport import TreeCoordinator,LocalBackend


def check_case(case_path,queries,output,k=10,candidate_budget=1000,scalar_rows=None):
    case_path=Path(case_path);case=json.loads((case_path/'case.json').read_text());tree=json.loads(Path(case['tree']).read_text())
    descriptors={slot['subtree_id']:dict(manifest=case['entries'][slot['subtree_id']]['copy']+'/manifest-'+str(slot['generation'])+'.json',checkpoint_dir=case['entries'][slot['subtree_id']]['copy'],binary='/mnt/spann/hierarchical_spann_shard-verified-v2',snapshot_sha256=slot['snapshot']['sha256']) for slot in tree['subtrees']}
    local_backend=LocalBackend(descriptors)
    local=TreeCoordinator.from_files(case['tree'],case_path/'placement-two-hosts.json',backend=local_backend)
    one=TreeCoordinator.from_files(case['tree'],case_path/'placement-cohost.json')
    two=TreeCoordinator.from_files(case['tree'],case_path/'placement-two-hosts.json')
    records=[]
    try:
        startup={label:c.verify_roots() for label,c in [('direct',local),('one_host',one),('two_hosts',two)]}
        for query in queries:
            for width in (1,2,None):
                budget=tree['source']['row_count'] if width is None else candidate_budget
                results={label:c.query(query['vector'],k,beam_width=width,candidate_budget=budget) for label,c in [('direct',local),('one_host',one),('two_hosts',two)]}
                expected=results['direct']
                for label,result in results.items():
                    for field in ('ids','levels','selected_candidates','reached_leaves','global_candidate_count'):
                        if result[field]!=expected[field]:raise AssertionError(label+' differs in '+field)
                    if not all(math.isclose(a,b,abs_tol=2e-5,rel_tol=2e-5) for a,b in zip(result['distances'],expected['distances'])):raise AssertionError('exact distance parity failed')
                    if result['global_candidate_count']>budget:raise AssertionError('global candidate budget exceeded')
                    if width is not None and any(level['selected_count']>width for level in result['levels']):raise AssertionError('global beam budget exceeded')
                    if len(result['ids'])!=len(set(result['ids'])):raise AssertionError('duplicate global result')
                if width==1 and len(results['two_hosts']['actual_hosts'])!=1:raise AssertionError('one selected slot should contact one physical owner')
                if width is None and scalar_rows is not None:
                    q=query['vector'];exact=sorted(((sum((a-b)**2 for a,b in zip(row,q)),i) for i,row in enumerate(scalar_rows)))[:k]
                    if expected['ids']!=[i for _,i in exact]:raise AssertionError('independent scalar exact IDs differ')
                    if not all(math.isclose(a,d,abs_tol=.002,rel_tol=1e-6) for a,(d,_) in zip(expected['distances'],exact)):raise AssertionError('independent scalar exact distances differ')
                records.append(dict(query_id=query['query_id'],beam_width=width,results=results))
        report=dict(status='passed',tree_sha256=case['tree_sha256'],tree_generation=tree['tree_generation'],query_count=len(queries),configurations=3,startup=startup,checks=['same committed tree','direct/one-host/two-host global frontier parity','global beam and candidate budget bounds','unique explicit global IDs','owner selectivity at beam1']+(['independent scalar exact oracle'] if scalar_rows is not None else []),records=records)
        Path(output).write_text(json.dumps(report,allow_nan=False)+'\n')
        return report
    finally:
        local.close();one.close();two.close()


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--case',required=True);p.add_argument('--queries',required=True);p.add_argument('--output',required=True);p.add_argument('--k',type=int,default=10);p.add_argument('--candidate-budget',type=int,default=1000);p.add_argument('--scalar-rows');a=p.parse_args()
    queries=[json.loads(line) for line in Path(a.queries).read_text().splitlines()]
    rows=json.loads(Path(a.scalar_rows).read_text()) if a.scalar_rows else None
    result=check_case(a.case,queries,a.output,a.k,a.candidate_budget,rows)
    print(json.dumps({k:v for k,v in result.items() if k not in ('records','startup')}))

if __name__=='__main__':main()
