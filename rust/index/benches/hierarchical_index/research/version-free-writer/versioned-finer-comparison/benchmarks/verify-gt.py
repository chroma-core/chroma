from pathlib import Path
import pyarrow.parquet as pq
import hashlib,json
r=Path('/mnt/data/versioned-finer-20261020');a=r/'artifacts'
old=Path('/mnt/msmarco/version-free-followup-20261020/artifacts/wikipedia-gt.parquet');new=a/'wikipedia-gt.parquet'
m=json.loads((a/'gt-verification.json').read_text());old_hash=hashlib.sha256(old.read_bytes()).hexdigest();assert old_hash==m['gt_sha256']
x=pq.read_table(old).to_pylist();y=pq.read_table(new).to_pylist();boundaries=sorted(set(v['max_vector_id'] for v in x));selected=[v for v in y if v['max_vector_id'] in boundaries];assert selected==x
result={'verified_original_path':str(old),'verified_original_sha256':old_hash,'used_gt_path':str(new),'used_gt_sha256':hashlib.sha256(new.read_bytes()).hexdigest(),'original_boundaries':boundaries,'used_boundaries':sorted(set(v['max_vector_id'] for v in y)),'original_rows':len(x),'used_rows':len(y),'exact_1m_3m_subset_equal_all_columns':True,'explanation':'The used container adds 9M ground-truth rows. Every original 1M/3M query vector and neighbor list matches the verified original container exactly.'}
(a/'gt-provenance-verification.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
