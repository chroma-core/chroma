import hashlib,json,pathlib,sys
import numpy as np
sys.path.insert(0,'/private/tmp/chroma-hierarchical-spann-sharding')
from tools.hierarchical_sharding.reference_bounds import interval_candidates,validate_and_rerank
source=pathlib.Path('/private/tmp/chroma-hierarchical-spann-sharding/tools/hierarchical_sharding/reference_bounds.py')
rng=np.random.default_rng(127)
for _ in range(200):
 exact=rng.normal(size=100);errors=rng.uniform(0,2,size=100);scores=exact+rng.uniform(-1,1,size=100)*errors;k=int(rng.integers(1,101))
 candidates,_=interval_candidates(scores,errors,k)
 assert set(np.argsort(exact)[:k])<=set(candidates)
x=np.array([[1e-40,0],[0,5e-38]],np.float32);q=np.array([1e38,1e35],np.float32)
approx=np.array([1,np.float32(1)-x[1]@q],np.float32)
result={'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'python':sys.version,'numpy':np.__version__,'valid_interval_randomized_cases_passed':200,'daz_contract_violation':{'selected':validate_and_rerank(x,q,approx,[0,1],1),'full_f64_distances':(1-x.astype(np.float64)@q.astype(np.float64)).tolist()},'wrong_length_accepted':validate_and_rerank([[1]],[1],[0,10],[7],1),'fractional_ids_truncated':validate_and_rerank([[1],[1]],[1],[0,0],[1.9,2.9],2)}
print(json.dumps(result,indent=2))
