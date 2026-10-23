"""Extract only declared document queries and compute new dense-prefix GT."""
import hashlib,json,math,os,struct,sys,time
from pathlib import Path
from harness_common import require,descriptor,write
from quality_analysis import cohort_ids,validate_gt
HERE=Path(__file__).resolve().parent

def prepare(source,out,*,rows=16384,dimension=768,source_rows=2000000,selected=None,k=100):
    started=time.monotonic();selected=cohort_ids() if selected is None else selected;row_bytes=dimension*4
    require(len(selected)>0 and len(selected)==len(set(selected)) and all(type(i) is int and rows<=i<source_rows for i in selected),'disjoint selected IDs')
    require(Path(source).stat().st_size==source_rows*row_bytes,'canonical extent')
    queries=[];hashes=set();read_bytes=0;row_records=[]
    with Path(source).open('rb') as stream:
        for j,identity in enumerate(selected):
            stream.seek(identity*row_bytes);raw=stream.read(row_bytes);require(len(raw)==row_bytes,'query extent');values=list(struct.unpack('<'+str(dimension)+'f',raw));require(all(math.isfinite(v) for v in values),'query finite')
            digest=hashlib.sha256(raw).hexdigest();require(digest not in hashes,'duplicate selected vector bits');hashes.add(digest);read_bytes+=len(raw)
            split='calibration' if j%9==0 else 'evaluation';qid='canonical-doc:'+str(identity);queries.append(dict(query_id=qid,canonical_global_id=identity,vector=values,split=split));row_records.append(dict(query_id=qid,canonical_global_id=identity,offset=identity*row_bytes,bytes=row_bytes,sha256=digest,split=split))
        target=out/'evaluation-prefix.f32';copied=0;prefix_hash=hashlib.sha256();requests=0
        stream.seek(0)
        with target.open('xb') as dst:
            while copied<rows*row_bytes:
                raw=stream.read(min(8192*row_bytes,rows*row_bytes-copied));require(raw and len(raw)%row_bytes==0,'prefix extent')
                for offset in range(0,len(raw),row_bytes):require(hashlib.sha256(raw[offset:offset+row_bytes]).hexdigest() not in hashes,'document bits duplicate indexed vector')
                dst.write(raw);prefix_hash.update(raw);copied+=len(raw);requests+=1
            dst.flush();os.fsync(dst.fileno())
    copy_seconds=time.monotonic()-started;sys.path.insert(0,str(HERE/'engine'))
    from hierarchical_sharding.exact_blocked import exact_blocked
    import numpy as np
    result=dict(status='passed',queries={},ground_truth={},prefix=descriptor(target),prefix_sha256=prefix_hash.hexdigest(),prefix_copy_bytes=copied,query_logical_read_bytes=read_bytes,query_selected_row_requests=len(selected),prefix_read_requests=requests,copy_extract_seconds=copy_seconds,row_provenance=row_records,numpy_version=np.__version__,scope='New exact FP64 prefix GT. Declared document IDs excluded from indexed prefix, exact vector duplicates rejected, semantic independence unproven. Logical bytes/requests, not physical IO.')
    gt_started=time.monotonic()
    for split in ['calibration','evaluation']:
        q=[v for v in queries if v['split']==split];path=out/(split+'-queries.jsonl');path.write_text(''.join(json.dumps(v)+'\n' for v in q));gt=exact_blocked(target,q,dimension,k=k,metric='ip',corpus_block=8192,query_block=16);validate_gt(gt,[v['query_id'] for v in q],rows,k);gtpath=out/(split+'-gt.jsonl');gtpath.write_text(''.join(json.dumps(v)+'\n' for v in gt));result['queries'][split]=descriptor(path);result['ground_truth'][split]=descriptor(gtpath)
    result.update(gt_seconds=time.monotonic()-gt_started,setup_seconds=time.monotonic()-started)
    write(out/'preparation.json',result);return result
