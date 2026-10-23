"""Bounded publication-query embedding epoch; no infrastructure is provisioned here."""
import argparse,json,time,gc
from pathlib import Path
import numpy as np
from .tree_manifest import artifact,canonical_bytes,_atomic_write
from .publication_queries import load_pinned_model,PinnedEncoder,compare_producers,produce,validate


def admit_work(seconds_per_item,count,remaining_s,reserve_s=300,margin=1.5):
    if not np.isfinite([seconds_per_item,remaining_s,reserve_s,margin]).all() or seconds_per_item<=0 or type(count) is not int or count<1 or reserve_s<300 or margin<1.5:
        raise ValueError('positive measured rate and conservative budget required')
    projected=seconds_per_item*count*margin
    if projected+reserve_s>remaining_s:raise TimeoutError('measured work does not fit fixed deadline plus preservation reserve')
    return dict(measured_seconds_per_item=seconds_per_item,items=count,margin=margin,reserve_s=reserve_s,remaining_s=remaining_s,admitted_projection_s=projected)


def legacy_probe(actual,expected):
    if actual.dtype!=np.float32 or expected.dtype!=np.float32 or actual.shape!=(16,768) or expected.shape!=(16,768):
        raise ValueError('legacy probe shape or dtype differs')
    validate(actual);validate(expected)
    delta=float(np.abs(actual.astype(np.float64)-expected).max())
    if not np.isfinite(delta) or delta>2e-5:raise ValueError('new runtime differs from preserved CPU producer')
    return delta


def embedding_job(texts,output,model_metadata,code_metadata,cache,lock,old_texts,old_vectors,stop_epoch):
    """Run pinned model probes, then admit restartable batches from measured throughput."""
    output=Path(output);output.mkdir(parents=True,exist_ok=False);started=time.monotonic();report=dict(status='running')
    try:
        report['admission_inputs']={name:artifact(Path(value)) for name,value in [('texts',texts),('old_texts',old_texts),('old_vectors',old_vectors),('model_metadata',model_metadata),('code_metadata',code_metadata),('runtime_lock',lock)]}
        if stop_epoch-time.time()<600:raise TimeoutError('insufficient slot for model probes and preservation')
        rows=[json.loads(line) for line in Path(texts).read_text().splitlines()]
        if len(rows)!=11000 or len({r['query_id'] for r in rows})!=11000:raise ValueError('frozen11000 distinct text queries required')
        old=[json.loads(line) for line in Path(old_texts).read_text().splitlines()]
        vectors=[json.loads(line) for line in Path(old_vectors).read_text().splitlines()]
        lookup={r['query_id']:r['query'] for r in old};old_ids={r['query_id'] for r in vectors}
        if len(old_ids)!=200 or old_ids!=set(lookup) or any(r['query_id'] in old_ids for r in rows):raise ValueError('publication pool must be disjoint from complete200 integration queries')
        begin=time.monotonic();model,runtime=load_pinned_model(json.loads(Path(model_metadata).read_text()),json.loads(Path(code_metadata).read_text()),cache,lock,device='cpu')
        report['model_download_and_load_s']=time.monotonic()-begin;encoder=PinnedEncoder(model)
        # Tokenize every frozen query before any bulk encoding; no truncation.
        tokenized=model.tokenizer([r['query'] for r in rows],padding=False,truncation=False)['input_ids']
        if len(tokenized)!=11000 or max(map(len,tokenized))>256:raise ValueError('publication tokenizer length gate failed')
        report['token_length_range']=[min(map(len,tokenized)),max(map(len,tokenized))]
        small=[r['query'] for r in rows[:128]];cpu=encoder(small)
        legacy=vectors[:16];legacy_actual=encoder([lookup[r['query_id']] for r in legacy]);legacy_expected=np.asarray([r['vector'] for r in legacy],np.float32)
        report['legacy_cpu_max_abs_difference']=legacy_probe(legacy_actual,legacy_expected)
        model.to('cuda');runtime['device']='cuda'
        report['producer_parity']=compare_producers(small,lambda _:cpu,encoder)
        import torch
        report['device']=torch.cuda.get_device_name(0);report['cuda']=torch.version.cuda
        report['batch_pilots']=[]
        for batch in (32,64,128):
            torch.cuda.reset_peak_memory_stats();begin=time.monotonic();value=PinnedEncoder(model,batch)(small);torch.cuda.synchronize();elapsed=time.monotonic()-begin
            if np.max(np.abs(value.astype(np.float64)-cpu))>2e-5:raise ValueError('batch pilot changes producer parity')
            report['batch_pilots'].append(dict(batch_size=batch,rows=len(small),wall_s=elapsed,gpu_peak_bytes=torch.cuda.max_memory_allocated()))
        measured=report['batch_pilots'][-1]['wall_s']/128
        report['admission']=admit_work(measured,len(rows),stop_epoch-time.time())
        runtime['gpu_probe']=report['producer_parity'];runtime['producer_sources']=[artifact(Path(__file__)),artifact(Path(__file__).with_name('publication_queries.py'))]
        result=produce(texts,output/'batches',PinnedEncoder(model,128),runtime,batch_size=128,max_wall_s=stop_epoch-time.time()-300)
        actual=[]
        for chunk in result['chunks']:
            path=output/'batches'/chunk['vectors']['path'];a=np.fromfile(path,dtype='<f4').reshape(len(chunk['query_ids']),768)
            actual.extend(dict(query_id=qid,vector=v.tolist()) for qid,v in zip(chunk['query_ids'],a))
        if [r['query_id'] for r in actual]!=[r['query_id'] for r in rows]:raise ValueError('materialized query order differs')
        _atomic_write(output/'queries.jsonl',''.join(json.dumps(r)+'\n' for r in actual).encode())
        report.update(status='passed',queries=11000,runtime=runtime,source_text=artifact(Path(texts)),output=artifact(output/'queries.jsonl',output),wall_s=time.monotonic()-started)
        _atomic_write(output/'complete.json',canonical_bytes(report));return report
    except BaseException as error:
        report.update(status='failed',error=repr(error),wall_s=time.monotonic()-started);raise
    finally:
        _atomic_write(output/'report.json',canonical_bytes(report))
        if 'model' in locals():del model
        gc.collect()
        if 'torch' in locals():torch.cuda.empty_cache()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ('texts','output','model-metadata','code-metadata','cache','lock','old-texts','old-vectors'):p.add_argument('--'+key,required=True)
    p.add_argument('--stop-epoch',required=True,type=float);a=p.parse_args()
    result=embedding_job(a.texts,a.output,a.model_metadata,a.code_metadata,a.cache,a.lock,a.old_texts,a.old_vectors,a.stop_epoch)
    print(json.dumps(dict(status=result['status'],queries=result['queries'],wall_s=result['wall_s'])))

if __name__=='__main__':main()
