"""Run only in the explicitly idle driver preparation slot."""
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time
sys.path.insert(0,'/mnt/spann/source')
os.environ.setdefault('OPENBLAS_NUM_THREADS','4')
import numpy as np
from tools.hierarchical_sharding.benchmark import exact_shard,read_jsonl
from tools.hierarchical_sharding.exact_blocked import exact_blocked
from tools.hierarchical_sharding.tree_manifest import artifact,file_hash

root=Path('/mnt/spann')
corpus=root/'fineweb-prefix-100k-v1/corpus.f32'
text=root/'fineweb-query-pools200/text/queries.text.jsonl'
frozen=root/'fineweb-query-pools200/frozen/frozen-pools.json'
output=root/'fineweb-pilot-preparation-v1'
output.mkdir(exist_ok=False)
report=dict(status='running',stages=[],source_corpus=artifact(corpus),frozen_pools=artifact(frozen),started_unix=time.time())
assert report['source_corpus']['sha256']=='5ba7887551c7a7a770a45675dc54aac37e44f8cc01329cb9bcdcecc964b3ce7c'
assert report['frozen_pools']['sha256']=='cb84821fab1169b58690ade3e2ab0855d9fe7205a52bd9355df84ca4a12822c8'

def command(stage,args):
    began=time.monotonic()
    with (output/(stage+'.log')).open('w') as stream:
        subprocess.run([sys.executable,'-m',*args],check=True,cwd='/mnt/spann/source',stdout=stream,stderr=subprocess.STDOUT)
    report['stages'].append(dict(stage=stage,wall_s=time.monotonic()-began,arguments=args))

try:
    command('embed',['tools.hierarchical_sharding.fineweb_queries','embed','--texts',str(text),'--output',str(output/'embeddings'),'--model-metadata',str(root/'query-model-files.json'),'--cache',str(root/'query-model-cache'),'--threads','4'])
    command('materialize',['tools.hierarchical_sharding.fineweb_queries','materialize','--embeddings',str(output/'embeddings/queries.jsonl'),'--frozen',str(frozen),'--output',str(output/'queries')])
    for pool in ('calibration','test'):
        command('exact-'+pool,['tools.hierarchical_sharding.exact_blocked','--vectors',str(corpus),'--queries',str(output/'queries'/(pool+'.jsonl')),'--dimension','768','--metric','ip','--k','100','--output',str(output/'queries'/(pool+'.exact.jsonl'))])
    # The scalar producer is independent of BLAS; it scores original source
    # values, with no normalization, under the same one-minus-dot metric.
    tiny=output/'oracle-prefix512.f32'
    with corpus.open('rb') as source:tiny.write_bytes(source.read(512*768*4))
    queries=read_jsonl(output/'queries/calibration.jsonl')[:3]
    scalar=exact_shard(tiny,queries,768,20,'ip')
    blocked=exact_blocked(tiny,queries,768,20,'ip',corpus_block=127,query_block=2)
    maximum=0.
    for a,b in zip(scalar,blocked):
        assert a['ids']==b['ids']
        for x,y in zip(a['distances'],b['distances']):
            maximum=max(maximum,abs(x-y));assert math.isclose(x,y,abs_tol=1e-12,rel_tol=1e-12)
    report['oracle_check']=dict(status='passed',corpus_rows=512,queries=3,k=20,metric='one_minus_inner_product',normalization='none',max_abs_distance_delta=maximum)
    command('spatial',['tools.hierarchical_sharding.spatial_prepare','--source',str(corpus),'--output',str(root/'fineweb-spatial100k-16-v1'),'--source-id','Qdrant/FineWeb-10B@3aea0b8d5f98741e343d07f067ce0f49dca1ccd2:lex-prefix100k:preserved-f16-to-f32','--construction-epoch','fineweb100k-16slots-v1','--dimension','768','--metric','ip','--children','16','--sample-size','8192'])
    report['references']={pool:artifact(output/'queries'/(pool+'.exact.jsonl')) for pool in ('calibration','test')}
    report['spatial_input']=artifact(root/'fineweb-spatial100k-16-v1/input.json')
    report['status']='passed'
except Exception as error:
    report.update(status='failed',error_type=type(error).__name__,error=str(error))
    raise
finally:
    report['finished_unix']=time.time()
    (output/'preparation-report.json').write_text(json.dumps(report,indent=2)+'\n')
