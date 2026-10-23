"""Admitted V5 reader replay against the unchanged retained sixteen-row fixture."""
import hashlib,json
from pathlib import Path
from tools.hierarchical_sharding.dynamic_runtime import DeploymentReader
from tools.hierarchical_sharding.dynamic_deployment import load_deployment
from tools.hierarchical_sharding.test_dynamic_trie_query import logical_trace
from tools.hierarchical_sharding.server import Reader
from tools.hierarchical_sharding.test_streaming_policy import LIMITS
HERE=Path(__file__).resolve().parent;binary='/private/tmp/spann-native-streaming-v5/hierarchical_spann_shard'
assert hashlib.sha256(Path(binary).read_bytes()).hexdigest()=='789873aa96046f713546f000ebedbfb35f170b1518bad67993dedfc1a767e790'
previous=HERE.parent/'streaming-postings-v1/runtime/streaming-full-query';pointer=previous/'current.json';head=pointer.read_bytes();opened=[];proof={};raw=[]
try:
 for mode in ('default','stream'):
  reader=DeploymentReader(pointer,binary,timeout=5,worker_id_mode='local-only',profile_memory=True,streaming_limits=LIMITS if mode=='stream' else None);opened.append(reader);proof[mode]=reader.query([4.5,1,0,0],1,beam_width=None,candidate_budget=1,timeout=5)
 assert logical_trace(proof['default'])==logical_trace(proof['stream'])
 old=json.loads((previous/'full-trace-and-budget.json').read_text());failure=next(x for x in old['raw_operations'] if 'error' in x['response']);sid=failure['subtree_id'];deployment=load_deployment(pointer);owner=deployment['owners'][sid];generation=next(s['generation'] for s in deployment['tree']['subtrees'] if s['subtree_id']==sid)
 direct=Reader([binary,'read','--checkpoint-dir',owner['checkpoint_dir'],'--generation',str(generation),'--local-ids-only'],5);opened.append(direct)
 def request(payload):
  response=direct.query(payload,timeout=5);raw.append(dict(request=payload,response=response));return response
 failing=dict(failure['request'],request_id='v5-overflow');error=request(failing);assert 'unique-candidate budget exceeded' in error['error']
 for key in ('materialized_leaves','posting_block_pins','vector_block_pins'):assert error['retained_postings'][key]==0
 retry=dict(failing,streaming_limits=LIMITS,request_id='v5-retry');actual=request(retry);assert 'error' not in actual
 for key in ('materialized_leaves','posting_block_pins','vector_block_pins'):assert actual['retained_postings'][key]==0
 oracle=dict(retry,request_id='v5-direct-oracle');oracle.pop('streaming_limits');expected=request(oracle);assert 'error' not in expected
 normalized=lambda row:sorted((c['local_id'],c['distance']) for c in row['candidates'])
 assert normalized(actual)==normalized(expected);assert actual['unique_candidates_scanned']==expected['unique_candidates_scanned'];assert pointer.read_bytes()==head
 proof.update(status='passed',direct_operations=raw,scope='one global M1 tie trace pair and same-process overflow/retry on retained snapshot; no writer/performance claim')
finally:
 for reader in reversed(opened):reader.close()
 (HERE/'replay-raw.json').write_text(json.dumps(proof,indent=2)+'\n')
print('PASS V5: full tied trace pair; same-process overflow then exact valid retry; zero streaming arrays/pins; published pointer unchanged')
