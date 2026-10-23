"""Create stage inputs from pinned successful restore evidence; execute nothing."""
import argparse
import hashlib
import json
import time
from pathlib import Path

PLAN = 'dd8efb14d493ef6b564b3c66d0f7198415b879c1aed932eb6ba3fe4581af8d53'
MATERIALIZER = '730b509fdd34b7cd1513fd5de2df148dfffc4a216739d879832c88b6b8da1e67'

def verify(binding):
    p = Path(binding['path'])
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1048576), b''):
            h.update(b)
    if p.stat().st_size != binding['bytes'] or h.hexdigest() != binding['sha256']:
        raise ValueError('bound input differs: '+str(p))
    return p

def create(spec):
    base = json.loads(verify(spec['base']).read_text())
    receipt = json.loads(verify(spec['restore_receipt']).read_text())
    runtime = json.loads(verify(spec['restore_runtime']).read_text())
    restore_spec = json.loads(verify(spec['restore_spec']).read_text())
    inputs = json.loads(verify(spec['matched_inputs']).read_text())
    if any(runtime.get(k) != v for k,v in receipt.items()):
        raise ValueError('restore receipt/runtime disagreement')
    if runtime.get('status') != 'complete' or runtime.get('plan_sha256') != PLAN:
        raise ValueError('unaccepted restore')
    if runtime.get('materializer_sha256') != MATERIALIZER or runtime.get('spec_sha256') != spec['restore_spec']['sha256']:
        raise ValueError('restore producer differs')
    if runtime['aws_identity']['Account'] != '079819354890':
        raise ValueError('wrong restore account')
    if (runtime['rows'],runtime['dimension'],runtime['expected_bytes']) != (2000000,768,6144000000):
        raise ValueError('wrong cohort')
    for k in ('deadline_unix','min_free_bytes'):
        if base[k] != runtime[k] or base[k] != restore_spec[k]:
            raise ValueError('restore/stage budget differs')
    if runtime['canonical']['bytes'] != runtime['expected_bytes']:
        raise ValueError('canonical extent differs')
    if time.time() >= base['deadline_unix']:
        raise ValueError('expired execution window')
    verify(runtime['canonical'])
    query = spec['queries']
    verify(query)
    if any(query[k] != inputs['queries'][k] for k in ('bytes','sha256')):
        raise ValueError('frozen query bytes differ')
    native = dict(format_version=1,source_id='fineweb-canonical2m-dynamic-v1',vectors=runtime['canonical'],
                  rows=2000000,dimension=768,metric='ip',config=inputs['config'],storage_chunk=8192,
                  balance_every=100000,owner_slots=10,owner_vector_threshold=250000,max_nodes=100000,
                  max_leaf_rows=262144,max_owner_physical_rows=4000000)
    if time.time() >= base['deadline_unix']:
        raise ValueError('window expired during verification')
    out = Path(spec['output']);out.mkdir(parents=True,exist_ok=False)
    for name,value in [('native-common.json',native),('prepare-stage.json',dict(base,vectors=runtime['canonical'],queries=query,rows=2000000,dimension=768,output=str(out/'prepared'))),('restore-bindings.json',spec)]:
        with (out/name).open('x') as f:json.dump(value,f,indent=2);f.write('\n')

    files = []
    for name in ('native-common.json','prepare-stage.json','restore-bindings.json'):
        path = out/name
        raw = path.read_bytes()
        files.append(dict(path=str(path.resolve()),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest()))
    with (out/'bound-inputs.json').open('x') as stream:
        json.dump(dict(format_version=1,files=files),stream,indent=2)
        stream.write('\n')

if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--spec',required=True)
    create(json.loads(Path(parser.parse_args().spec).read_text()))
