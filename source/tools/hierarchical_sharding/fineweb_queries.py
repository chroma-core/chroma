"""Small pinned FineWeb query recovery and CPU embedding checks; no recall claim."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import random
import subprocess
import sys
import time
import numpy as np
from .fineweb_prepare import RangeReader,REVISION
from .tree_manifest import artifact,canonical_bytes,file_hash

MODEL='Alibaba-NLP/gte-multilingual-base'
MODEL_REVISION='9bbca17d9273fd0d03d5725c7a4b0f6b45142062'
CODE_REVISION='40ced75c3017eb27626c9d4ea981bde21a2662f4'
QUERY_REVISION='a47ee7aae8d7d466ba15f9f0bfac3b3681087b3a'
TEXT_HASH='3a354db0c5aa2bcf5d4563844c645d8fc23ff1780f83cce28900b7fc917a5756'
GT=dict(url=f'https://huggingface.co/datasets/Qdrant/FineWeb-10B/resolve/{REVISION}/queries/gt_dense_k1000.parquet',bytes=2126961576,publisher_sha256='847989d235ecd8c7118b64709cfe5d54ff36b82ad8c70c9de31c5347633bbd81')
SOURCE=dict(url=f'https://huggingface.co/datasets/microsoft/ms_marco/resolve/{QUERY_REVISION}/v2.1/test-00000-of-00001.parquet',bytes=204396131,publisher_sha256='bfbf0c91c55f85176a25760d12ec8daf9f222305bebb69025700c2147931c2a8')


def recover_rows(identities,source_rows):
    lookup={}
    for row in source_rows:
        key=str(row['query_id'])
        if key in lookup:raise ValueError('duplicate source query ID')
        if not isinstance(row['query'],str):raise ValueError('invalid source query text')
        lookup[key]=row['query']
    result=[];seen=set();h=hashlib.sha256()
    for row in identities:
        if row['msmarco_config']!='v2.1' or row['msmarco_split']!='test':raise ValueError('unexpected dense query source split')
        qid=str(row['msmarco_query_id'])
        if qid in seen:raise ValueError('duplicate published dense query identity')
        seen.add(qid);text=lookup[qid];h.update(text.encode());h.update(b'\0')
        result.append(dict(query_id='v2.1:test:'+qid,msmarco_query_id=qid,msmarco_config='v2.1',msmarco_split='test',query=text))
    return result,h.hexdigest()


def recover(output,count=16,seed=1729,max_source_bytes=64*1024*1024,max_wall_s=120,exclude_publisher_rows=()):
    import pyarrow.parquet as pq
    if not 1<=count<=100000:raise ValueError('text selection requires 1 through 100000 queries')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);start=time.monotonic()
    budget=dict(max_bytes=max_source_bytes,bytes_read=0,deadline=start+max_wall_s)
    records=[];evidence=[]
    for desc,columns in [(GT,['msmarco_config','msmarco_split','msmarco_query_id']),(SOURCE,['query_id','query'])]:
        with RangeReader(desc['url'],desc['bytes'],budget=budget) as reader:
            table=pq.ParquetFile(reader).read(columns=columns,use_threads=False)
            records.append(table.to_pylist())
            evidence.append(dict(source=desc,columns=columns,ranges=reader.ranges,etag=reader.etag,publisher_whole_file_hash_locally_verified=False))
    rows,text_hash=recover_rows(*records)
    if len(rows)!=100000 or text_hash!=TEXT_HASH:raise ValueError('publisher full query-text checksum mismatch')
    excluded=set(exclude_publisher_rows)
    if any(not isinstance(i,int) or i<0 or i>=len(rows) for i in excluded):raise ValueError('invalid excluded publisher row')
    population=[i for i in range(len(rows)) if i not in excluded]
    if count>len(population):raise ValueError('insufficient distinct queries after exclusions')
    indices=sorted(random.Random(seed).sample(population,count));selected=[dict(rows[i],publisher_row=i) for i in indices]
    (output/'queries.text.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in selected))
    report=dict(status='passed',full_query_pool_rows=len(rows),full_query_text_sha256=text_hash,selected_queries=count,selected_publisher_rows=indices,
        seed=seed,excluded_publisher_rows=sorted(excluded),source_bytes_fetched=budget['bytes_read'],sources=evidence,wall_s=time.monotonic()-start,
        text_artifact=artifact(output/'queries.text.jsonl',output),scope='text identities and hashes verified; no embeddings or recall')
    (output/'text-provenance.json').write_bytes(canonical_bytes(report));return report


def freeze_pools(texts,output,calibration_count=100,test_count=100,seed=1729):
    from .benchmark import split_queries
    texts=Path(texts);rows=[json.loads(line) for line in texts.read_text().splitlines()]
    if calibration_count+test_count!=len(rows):raise ValueError('freeze requires exactly the selected query pool')
    calibration,test=split_queries(rows,calibration_count,test_count,seed)
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    for name,pool in [('calibration',calibration),('test',test)]:
        (output/(name+'.text.jsonl')).write_text(''.join(json.dumps(row)+'\n' for row in pool))
    result=dict(format_version=1,selection_policy='uniform source query selection, then seeded calibration/test shuffle before performance tuning',
        seed=seed,source_text=artifact(texts),calibration_ids=[r['query_id'] for r in calibration],test_ids=[r['query_id'] for r in test],
        text_artifacts={name:artifact(output/(name+'.text.jsonl'),output) for name in ('calibration','test')})
    (output/'frozen-pools.json').write_bytes(canonical_bytes(result));return result


def materialize_pools(embeddings,frozen,output):
    embeddings,frozen=Path(embeddings),Path(frozen);pool=json.loads(frozen.read_text())
    rows=[json.loads(line) for line in embeddings.read_text().splitlines()];lookup={r['query_id']:r for r in rows}
    expected=pool['calibration_ids']+pool['test_ids']
    if len(set(expected))!=len(expected) or len(lookup)!=len(rows) or set(lookup)!=set(expected):raise ValueError('embedding identities differ from the frozen disjoint pools')
    if any(len(r['vector'])!=768 or not np.isfinite(r['vector']).all() for r in rows):raise ValueError('invalid query embeddings')
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    for name in ('calibration','test'):
        (output/(name+'.jsonl')).write_text(''.join(json.dumps(lookup[qid])+'\n' for qid in pool[name+'_ids']))
    result=dict(frozen_pools=artifact(frozen),embeddings=artifact(embeddings),pool_sizes={name:len(pool[name+'_ids']) for name in ('calibration','test')},
        outputs={name:artifact(output/(name+'.jsonl'),output) for name in ('calibration','test')})
    (output/'query-pools-provenance.json').write_bytes(canonical_bytes(result));return result


def embed(texts,output,model_metadata,cache,threads=4,batch_size=128):
    import torch
    import transformers
    import sentence_transformers
    from huggingface_hub import snapshot_download
    from sentence_transformers import SentenceTransformer
    texts=Path(texts);rows=[json.loads(line) for line in texts.read_text().splitlines()]
    if not 1<=len(rows)<=200:raise ValueError('this CPU smoke check is limited to 1 through 200 queries')
    metadata=json.loads(Path(model_metadata).read_text())
    if metadata['sha']!=MODEL_REVISION:raise ValueError('model metadata pin mismatch')
    wanted=[r for r in metadata['siblings'] if r['rfilename'].endswith(('.json','.safetensors','.model','.txt')) and not r['rfilename'].startswith('onnx/')]
    if sum(r.get('size',0) for r in wanted)>700*1024*1024:raise ValueError('model download exceeds 700 MiB budget')
    output=Path(output);output.mkdir(parents=True,exist_ok=False);start=time.monotonic()
    lock=subprocess.check_output([sys.executable,'-m','pip','freeze','--all'],text=True)
    (output/'runtime.lock.txt').write_text(lock)
    snapshot=Path(snapshot_download(MODEL,revision=MODEL_REVISION,cache_dir=cache,allow_patterns=[r['rfilename'] for r in wanted],max_workers=2))
    files=[]
    for row in wanted:
        path=snapshot/row['rfilename'];desc=artifact(path)
        if row.get('lfs',{}).get('sha256') and desc['sha256']!=row['lfs']['sha256']:raise ValueError('downloaded model/tokenizer digest mismatch')
        files.append(dict(source_path=row['rfilename'],**desc))
    torch.set_num_threads(threads);torch.set_num_interop_threads(1);torch.manual_seed(1729);torch.use_deterministic_algorithms(True)
    model=SentenceTransformer(str(snapshot),trust_remote_code=True,device='cpu',
        model_kwargs=dict(code_revision=CODE_REVISION,torch_dtype=torch.float32),config_kwargs=dict(code_revision=CODE_REVISION))
    model.float();model.eval()
    code_module=type(model[0].auto_model).__module__
    if CODE_REVISION not in code_module:raise ValueError('loaded remote implementation revision is not pinned')
    kwargs=dict(batch_size=batch_size,convert_to_numpy=True,show_progress_bar=False,normalize_embeddings=False)
    begin=time.monotonic();a=model.encode([r['query'] for r in rows],**kwargs).astype('<f4');first_s=time.monotonic()-begin
    b=model.encode([r['query'] for r in rows],**kwargs).astype('<f4')
    if a.shape!=(len(rows),768) or not np.isfinite(a).all():raise ValueError('invalid dense query embeddings')
    norms=np.linalg.norm(a.astype(np.float64),axis=1)
    if not np.allclose(norms,1,atol=1e-5):raise ValueError('model output is not unit normalized')
    max_delta=float(np.max(np.abs(a-b)))
    if not np.array_equal(a,b):raise ValueError('repeated CPU encoding is not bit-identical')
    a.tofile(output/'queries.f32')
    (output/'queries.jsonl').write_text(''.join(json.dumps(dict(query_id=r['query_id'],publisher_row=r['publisher_row'],vector=v.tolist()))+'\n' for r,v in zip(rows,a)))
    code_files=[]
    for name,module in list(sys.modules.items()):
        if CODE_REVISION in name and getattr(module,'__file__',None):code_files.append(artifact(module.__file__))
    report=dict(status='passed',queries=len(rows),dimension=768,model=MODEL,model_revision=MODEL_REVISION,remote_code_revision=CODE_REVISION,
        loaded_remote_module=code_module,model_files=files,remote_code_files=code_files,device='cpu',dtype='float32',threads=threads,batch_size=batch_size,
        actual_smoke_batch_rows=len(rows),query_prefix=None,extra_normalization=False,model_contains_normalize_module=any(type(m).__name__=='Normalize' for m in model),
        norm_range=[float(norms.min()),float(norms.max())],repeat_max_abs_delta=max_delta,repeat_bit_identical=True,first_encode_s=first_s,wall_s=time.monotonic()-start,
        versions=dict(python=sys.version,torch=torch.__version__,transformers=transformers.__version__,sentence_transformers=sentence_transformers.__version__,numpy=np.__version__),
        platform=platform.platform(),runtime_lock=artifact(output/'runtime.lock.txt',output),source_text=artifact(texts),
        outputs={name:artifact(output/name,output) for name in ('queries.f32','queries.jsonl')},
        published_reference_parity_verified=False,scope='bounded CPU embedding reproducibility smoke check; full publisher batch/query parity and subset exact references remain unverified')
    (output/'embedding-provenance.json').write_bytes(canonical_bytes(report));return report


def main():
    p=argparse.ArgumentParser(description=__doc__);sub=p.add_subparsers(dest='command',required=True)
    text=sub.add_parser('recover');text.add_argument('--output',required=True);text.add_argument('--count',type=int,default=16);text.add_argument('--seed',type=int,default=1729);text.add_argument('--max-source-bytes',type=int,default=64*1024*1024);text.add_argument('--max-wall-s',type=float,default=120)
    freeze=sub.add_parser('freeze')
    for key in ('texts','output'):freeze.add_argument('--'+key,required=True)
    freeze.add_argument('--seed',type=int,default=1729);freeze.add_argument('--calibration-count',type=int,default=100);freeze.add_argument('--test-count',type=int,default=100)
    pools=sub.add_parser('materialize')
    for key in ('embeddings','frozen','output'):pools.add_argument('--'+key,required=True)
    model=sub.add_parser('embed')
    for key in ('texts','output','model-metadata','cache'):model.add_argument('--'+key,required=True)
    model.add_argument('--threads',type=int,default=4)
    a=p.parse_args()
    if a.command=='recover':r=recover(a.output,a.count,a.seed,max_source_bytes=a.max_source_bytes,max_wall_s=a.max_wall_s)
    elif a.command=='freeze':r=freeze_pools(a.texts,a.output,a.calibration_count,a.test_count,a.seed)
    elif a.command=='materialize':r=materialize_pools(a.embeddings,a.frozen,a.output)
    else:r=embed(a.texts,a.output,a.model_metadata,a.cache,a.threads)
    print(json.dumps(dict(status=r.get('status','passed'),queries=r.get('queries',r.get('selected_queries')),wall_s=r.get('wall_s'))))

if __name__=='__main__':main()
