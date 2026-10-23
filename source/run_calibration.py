"""Execute only the frozen calibration grid, preserving untouched held-out queries."""
import pathlib,json,sys,hashlib
source='/mnt/spann/owner-reduction-source-v2';sys.path.insert(0,source)
from tools.hierarchical_sharding.subtree_benchmark import curve
base=pathlib.Path('/mnt/spann');plan=json.loads((base/'read-grid-v1.json').read_text());settings=[(b,m) for b in plan['beam_widths'] for m in plan['candidate_budgets']]
assert plan['primary_recall_target']==.95 and settings==[(b,m) for b in (256,512,1024,2048) for m in (1000,4000)]
r=curve(base/'results/fineweb10m128-h2-a16-c16-v2',base/'fineweb10m-pilot-data',base/'results/fineweb10m-calibration-v1',settings=settings,target=.95,calibration_only=True);print(json.dumps(r),flush=True)
