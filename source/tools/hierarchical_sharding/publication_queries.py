"""Immutable resumable query batches; model execution is injected and pinned separately."""
import json
import time
from pathlib import Path
import numpy as np
from .tree_manifest import artifact, canonical_bytes, checked_artifact, _atomic_write
from .fineweb_queries import MODEL, MODEL_REVISION, CODE_REVISION


def _produce(text_path, output, encode, runtime, batch_size=128, max_wall_s=600):
    """Resume verified complete batches only, without changing source order or semantics."""
    if type(batch_size) is not int or not 1 <= batch_size <= 128:
        raise ValueError('batch size must be an integer in 1..128')
    if max_wall_s <= 0:
        raise ValueError('positive runtime budget required')
    required = dict(model=MODEL, model_revision=MODEL_REVISION,
                    remote_code_revision=CODE_REVISION, dtype='float32',
                    query_prefix=None, extra_normalization=False)
    if any(runtime.get(key) != value for key, value in required.items()):
        raise ValueError('producer runtime does not bind the publisher semantics')
    if not runtime.get('runtime_lock_sha256') or not runtime.get('model_files') or not runtime.get('remote_code_files'):
        raise ValueError('runtime and actual loaded file digests are required')
    text_path, output = Path(text_path), Path(output)
    rows = [json.loads(line) for line in text_path.read_text().splitlines()]
    ids = [row['query_id'] for row in rows]
    if not rows or len(rows) > 11000 or len(set(ids)) != len(ids):
        raise ValueError('query IDs must be distinct with at most 11000 rows')
    if any(not isinstance(row.get('query'), str) for row in rows):
        raise ValueError('query text must be a string')
    output.mkdir(parents=True, exist_ok=True)
    identity = dict(format_version=1, source=artifact(text_path), runtime=runtime,
                    batch_size=batch_size, dimension=768, query_ids=ids)
    state_path = output/'identity.json'
    if state_path.exists():
        if json.loads(state_path.read_text()) != identity:
            raise ValueError('resume changes source, runtime or batch settings')
    else:
        if any(p.name != '.writer.lock' for p in output.iterdir()):
            raise ValueError('new output contains unrecognized files')
        _atomic_write(state_path, canonical_bytes(identity))
    started = time.monotonic(); chunks = []; resumed = 0
    try:
        for first in range(0, len(rows), batch_size):
            last = min(first+batch_size, len(rows)); receipt = output/f'chunk-{first:05d}.json'
            batch_ids = ids[first:last]
            if receipt.exists():
                saved = json.loads(receipt.read_text())
                if saved['first'] != first or saved['query_ids'] != batch_ids:
                    raise ValueError('completed batch identity mismatch')
                vector_path = checked_artifact(saved['vectors'], output)
                a = np.fromfile(vector_path, dtype='<f4').reshape(last-first,768)
                resumed += last-first
            else:
                if time.monotonic()-started >= max_wall_s:
                    raise TimeoutError('embedding budget exhausted before next batch')
                a = np.asarray(encode([r['query'] for r in rows[first:last]]))
                if a.dtype != np.float32 or a.shape != (last-first,768):
                    raise ValueError('encoder must return float32 768-dimensional rows')
                # Encode must reject oversized token sequences rather than truncate.
                path = output/f'chunk-{first:05d}.f32'
                if path.exists():
                    raise ValueError('uncommitted batch exists; retain it in a separate failed attempt')
                validate(a)
                with path.open('xb') as stream:
                    stream.write(a.astype('<f4',copy=False).tobytes())
                    stream.flush()
                    import os
                    os.fsync(stream.fileno())
                saved = dict(first=first, query_ids=batch_ids, vectors=artifact(path,output))
                _atomic_write(receipt,canonical_bytes(saved))
            validate(a); chunks.append(saved)
        result = dict(status='passed', identity=artifact(state_path,output), chunks=chunks,
                      query_count=len(rows), resumed_rows=resumed, wall_s=time.monotonic()-started)
        complete=output/'complete.json'
        if not complete.exists(): _atomic_write(complete,canonical_bytes(result))
        return result
    except BaseException as error:
        # Never overwrite a previous failed attempt or its evidence.
        attempt=0
        while (output/f'failure-{attempt:04d}.json').exists():attempt+=1
        _atomic_write(output/f'failure-{attempt:04d}.json',canonical_bytes(dict(status='failed',error=repr(error),completed_chunks=len(chunks),wall_s=time.monotonic()-started)))
        raise


def validate(a):
    if not np.isfinite(a).all():raise ValueError('nonfinite query embedding')
    norms=np.linalg.norm(a.astype(np.float64),axis=1)
    if not np.allclose(norms,1,atol=1e-5,rtol=0):raise ValueError('query embedding norm differs from publisher contract')


class PinnedEncoder:
    """Apply publisher encoding semantics after a separately verified model load."""
    def __init__(self, model, batch_size=128):
        if type(batch_size) is not int or not 1 <= batch_size <= 128:
            raise ValueError('batch size must be in 1..128')
        self.model=model;self.batch_size=batch_size

    def __call__(self, texts):
        tokens=self.model.tokenizer(texts, padding=False, truncation=False)
        if len(tokens['input_ids']) != len(texts) or any(len(ids)>256 for ids in tokens['input_ids']):
            raise ValueError('pinned tokenizer sequence exceeds the untruncated safety gate')
        values=self.model.encode(texts,batch_size=self.batch_size,convert_to_numpy=True,
                                 show_progress_bar=False,normalize_embeddings=False)
        return np.asarray(values)


def produce(text_path, output, encode, runtime, batch_size=128, max_wall_s=600):
    """Use one local writer; a second process cannot mutate the same batch directory."""
    import fcntl
    output=Path(output);output.mkdir(parents=True,exist_ok=True)
    with (output/'.writer.lock').open('a') as lock:
        try:fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError as error:raise ValueError('another query producer owns this output') from error
        return _produce(text_path,output,encode,runtime,batch_size,max_wall_s)


def selected_files(metadata, code=False):
    from .fineweb_queries import MODEL_REVISION, CODE_REVISION
    if metadata.get('sha') != (CODE_REVISION if code else MODEL_REVISION):
        raise ValueError('publisher metadata revision mismatch')
    suffixes=('.py',) if code else ('.json','.safetensors','.model','.txt')
    files=[r for r in metadata['siblings'] if r['rfilename'].endswith(suffixes) and not r['rfilename'].startswith('onnx/')]
    if not files or any(type(r.get('size')) is not int or r['size']<0 or not r['rfilename'] or Path(r['rfilename']).is_absolute() or '..' in Path(r['rfilename']).parts for r in files):
        raise ValueError('publisher file sizes and safe relative paths required')
    return files


def load_pinned_model(model_metadata, code_metadata, cache, lock_path, device='cuda', threads=4):
    """Download only listed publisher files, then execute pinned eager float32 code."""
    import os,hashlib,subprocess,sys
    from .tree_manifest import file_hash
    model_rows=selected_files(model_metadata);code_rows=selected_files(code_metadata,True)
    total=sum(r['size'] for r in model_rows+code_rows)
    if total>700*1024**2:raise ValueError('pinned model/tokenizer/code exceeds 700MiB cap')
    os.environ['CUBLAS_WORKSPACE_CONFIG']=':4096:8'
    import torch
    import sentence_transformers
    import transformers
    from huggingface_hub import snapshot_download
    from sentence_transformers import SentenceTransformer
    def download(repo,revision,rows):
        directory=Path(snapshot_download(repo,revision=revision,cache_dir=cache,allow_patterns=[r['rfilename'] for r in rows],max_workers=2))
        files=[]
        for row in rows:
            path=directory/row['rfilename'];body=path.read_bytes()
            if len(body)!=row['size']:raise ValueError('publisher file byte count mismatch')
            if row.get('lfs'):
                if hashlib.sha256(body).hexdigest()!=row['lfs']['sha256']:raise ValueError('publisher LFS digest mismatch')
            elif hashlib.sha1(b'blob '+str(len(body)).encode()+b'\0'+body).hexdigest()!=row['blobId']:
                raise ValueError('publisher Git blob digest mismatch')
            files.append(dict(source_path=row['rfilename'],**artifact(path)))
        return directory,files
    snapshot,model_files=download(MODEL,MODEL_REVISION,model_rows)
    _,code_files=download('Alibaba-NLP/new-impl',CODE_REVISION,code_rows)
    torch.set_num_threads(threads);torch.set_num_interop_threads(1);torch.manual_seed(1729)
    torch.use_deterministic_algorithms(True)
    torch.backends.cuda.matmul.allow_tf32=False;torch.backends.cudnn.allow_tf32=False
    torch.backends.cuda.matmul.allow_fp16_reduced_precision_reduction=False
    torch.backends.cuda.matmul.allow_bf16_reduced_precision_reduction=False
    torch.set_float32_matmul_precision('highest')
    model=SentenceTransformer(str(snapshot),trust_remote_code=True,device=device,
        model_kwargs=dict(code_revision=CODE_REVISION,torch_dtype=torch.float32,attn_implementation='eager'),
        config_kwargs=dict(code_revision=CODE_REVISION))
    model.float();model.eval()
    if getattr(model[0].auto_model.config,'_attn_implementation',None)!='eager':raise ValueError('loaded attention implementation is not eager')
    if any(p.is_floating_point() and p.dtype!=torch.float32 for p in model.parameters()):raise ValueError('loaded floating parameter is not float32')
    module=type(model[0].auto_model).__module__
    if CODE_REVISION not in module:raise ValueError('loaded implementation revision mismatch')
    loaded=[]
    expected={r['sha256'] for r in code_files}
    for name,item in list(sys.modules.items()):
        if CODE_REVISION in name and getattr(item,'__file__',None):
            desc=artifact(item.__file__)
            if desc['sha256'] not in expected:raise ValueError('loaded code differs from checked publisher files')
            loaded.append(desc)
    if not loaded:raise ValueError('no pinned implementation loaded')
    runtime=dict(model=MODEL,model_revision=MODEL_REVISION,remote_code_revision=CODE_REVISION,
        dtype='float32',query_prefix=None,extra_normalization=False,device=device,
        attention_implementation=getattr(model[0].auto_model.config,'_attn_implementation',None),
        runtime_lock_sha256=file_hash(lock_path),model_files=model_files,remote_code_files=loaded,
        declared_download_bytes=total,torch=torch.__version__,transformers=transformers.__version__,
        sentence_transformers=sentence_transformers.__version__,numpy=np.__version__,
        installed_freeze=subprocess.check_output([sys.executable,'-m','pip','freeze','--all'],text=True))
    return model,runtime


def compare_producers(texts,cpu_encode,gpu_encode):
    """Compare independent CPU/GPU execution before admitting a new embedding epoch."""
    a=np.asarray(cpu_encode(texts));b=np.asarray(gpu_encode(texts));c=np.asarray(gpu_encode(texts))
    if any(v.dtype!=np.float32 or v.shape!=(len(texts),768) for v in (a,b,c)):
        raise ValueError('probe shape or dtype mismatch')
    for value in (a,b,c):validate(value)
    delta=float(np.abs(a.astype(np.float64)-b).max())
    cosine=np.sum(a.astype(np.float64)*b,axis=1)/(np.linalg.norm(a.astype(np.float64),axis=1)*np.linalg.norm(b.astype(np.float64),axis=1))
    if delta>2e-5 or float(cosine.min())<1-1e-6 or not np.array_equal(b,c):
        raise ValueError('CPU/GPU parity or repeated GPU determinism gate failed')
    return dict(status='passed',queries=len(texts),max_absolute_difference=delta,min_cosine=float(cosine.min()),gpu_repeat_bit_identical=True)
