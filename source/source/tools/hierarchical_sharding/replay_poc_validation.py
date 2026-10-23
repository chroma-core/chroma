"""Replay retained transitions under an explicit immutable-input POC assumption."""
import argparse
import builtins
import copy
import hashlib
import io
import json
import signal
import struct
import subprocess
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from .dynamic_deployment import validate_record
from .tree_manifest import content_hash,load_model
from .measure_validation_reads import Reads
from .measure_rerank_pins import inventory
from .poc_incremental_validation import FixedInputSession


def main():
    parser=argparse.ArgumentParser()
    for name in ('fixture','output','binary','manifest'):parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);a=parser.parse_args()
    root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2]
    if out==root or root in out.parents or out in root.parents:raise ValueError('output overlaps fixture')
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    pins=json.loads(a.manifest.read_bytes())['files']
    if {p:sha(repo/p) for p in pins}!=pins or sha(a.binary)!=a.expected_binary_sha256:raise ValueError('source/binary admission differs')
    # Resolve stdlib temporary-directory probes before wrapping fixture reads.
    temporary_directory=tempfile.gettempdir()
    before=inventory(root);out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    receipt=json.loads((root/'receipt.json').read_bytes());parent=json.loads((root/'parent.json').read_bytes())
    appended=json.loads(Path(receipt['append_bundle']['path']).read_bytes())
    split_prepared=json.loads((root/'topology/split/prepared.json').read_bytes());split=json.loads(Path(split_prepared['descriptor']['path']).read_bytes())
    if parent['tree']['source']['row_count']!=96 or appended['deployment']['tree']['source']['row_count']!=98:raise ValueError('exact retained transition required')
    old_path=Path(parent['canonical_vectors']['path']);new_path=Path(appended['deployment']['canonical_vectors']['path'])
    tail=new_path.read_bytes()[96*32*4:];old=old_path.read_bytes()
    if len(old)!=96*32*4 or len(tail)!=2*32*4:raise ValueError('fixed overlay shape differs')
    base_span=dict(path=str(old_path),global_start=0,rows=96,source_offset_bytes=0,bytes=len(old),sha256=hashlib.sha256(old).hexdigest())
    tail_span=dict(path=str(new_path),global_start=96,rows=2,source_offset_bytes=96*32*4,bytes=len(tail),sha256=hashlib.sha256(tail).hexdigest())
    views={96:dict(assumption='fixed-immutable-input-poc',dimension=32,rows=96,spans=[base_span]),98:dict(assumption='fixed-immutable-input-poc',dimension=32,rows=98,spans=[base_span,tail_span])}
    # Setup reads/hashes are outside measured phases and disclosed separately.
    setup=dict(tempfile_directory_initialized_outside_measurement=temporary_directory,old_canonical_bytes=len(old),tail_bytes=len(tail),tail_container_bytes=new_path.stat().st_size,setup_old_canonical_returned_bytes=len(old),setup_tail_container_returned_bytes=new_path.stat().st_size,setup_scope='whole old96 and whole tail-container98 read before phases; not just256tailbytes; complete inventory hashes additionally excluded',fixture_files=len(before),fixture_bytes=sum(v['bytes'] for v in before.values()))
    records={'bootstrap-parent96':parent,'oracle-append98':appended['deployment'],'oracle-split98':split['deployment']}
    result=dict(status='failed',assumption='fixed saved inputs immutable for experiment; no production safety claim',setup=setup,phases={},copies=dict(canonical_materialization_bytes=0,checkpoint_clone_bytes=0,scope='validation/input-view replay only; actual writer copies remain'))
    native_calls=[];started=time.monotonic()
    def stop(*_):raise TimeoutError('POC replay work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(90)
    def native(phase,sid,slot,owner,ids,record):
        index=len(native_calls);stem=out/f'native-{index:02d}-{phase}-{sid}'
        selected=[]
        if record is appended['deployment'] and sid=='b':
            plan=json.loads(Path(appended['writer_execution']['path']).read_bytes());selected=[(r['leaf_id'],r['local_id']) for r in plan['routes'] if r['subtree_id']==sid]
        spec=dict(assumption='fixed-immutable-input-poc',checkpoint_dir=owner['checkpoint_dir'],generation=slot['generation'],manifest_sha256=slot['snapshot']['sha256'],map=slot['local_to_global']['path'],map_sha256=slot['local_to_global']['sha256'],view=views[record['tree']['source']['row_count']],selected=selected,subtree_id=sid,source_id=record['tree']['source']['source_id'],metric=record['tree']['metric'],routing_boundary_hash=slot['routing_boundary_hash'],root_ref=slot['root_ref'])
        spec_path=stem.with_suffix('.spec.json');spec_path.write_text(json.dumps(spec)+'\n')
        command=[str(a.binary.resolve()),'poc-audit-snapshot','--spec',str(spec_path)]
        with stem.with_suffix('.stdout').open('xb') as stdout,stem.with_suffix('.stderr').open('xb') as stderr:process=subprocess.run(command,stdout=stdout,stderr=stderr,timeout=10)
        if process.returncode or stem.with_suffix('.stdout').stat().st_size>1024*1024:raise ValueError('native saved-owner audit failed')
        response=json.loads(stem.with_suffix('.stdout').read_bytes())
        if response['status']!='audited-fixed-input' or response['global_ids']!=ids or response['snapshot_sha256']!=slot['snapshot']['sha256'] or response['map_sha256']!=slot['local_to_global']['sha256'] or response['selected']!=[list(r) for r in selected]:raise ValueError('actual saved-owner proof differs')
        native_calls.append(dict(phase=phase,owner=sid,command=command,response=response));return response
    def phase(name,work):
        # Existing wrapper counts Python-returned bytes; native bytes have their
        # own storage observer. Temporary bitmap mmap stays explicitly excluded.
        counter=Reads();ordinary,path_open=builtins.open,io.open;counter.started=time.monotonic()
        with patch('builtins.open',lambda *args,**kw:counter.opened(ordinary,*args,**kw)),patch('io.open',lambda *args,**kw:counter.opened(path_open,*args,**kw)):value=work()
        result['phases'][name]=dict(value=value,python=dict(events=counter.events,returned_bytes=counter.returned_bytes,rows=[dict(path=p,phase=f,**v) for (p,f),v in sorted(counter.rows.items())],excluded_temporaries=counter.excluded_temporaries))
        return value
    def oracle(name,record):
        def work():
            report=validate_record(record)
            for slot in record['tree']['subtrees']:
                path=Path(slot['local_to_global']['path']);ids=[v[0] for v in struct.iter_unpack('<Q',path.read_bytes())]
                native(name,slot['subtree_id'],slot,record['owners'][slot['subtree_id']],ids,record)
            return report
        return phase(name,work)
    try:
        bootstrap=oracle('bootstrap-parent96',parent);session=FixedInputSession(parent,bootstrap)
        for name,record,kind,affected in [('incremental-append98',appended['deployment'],'append',['b']),('incremental-split98',split['deployment'],'topology',['a'])]:
            value=phase(name,lambda record=record,kind=kind,affected=affected,name=name:session.transition(record,kind,affected,lambda sid,slot,owner,ids:native(name,sid,slot,owner,ids,record)))
            full=oracle('oracle-append98' if kind=='append' else 'oracle-split98',record)
            if any(value[key]!=full[key] for key in ('tree_sha256','vector_count','subtrees')):raise ValueError('incremental/full logical report differs')
            # The output references the complete actual candidate owners. Legacy
            # record/page/trie sidecars are checked only by the separate oracle.
            if value['owner_payloads']!={s['subtree_id']:{k:s[k] for k in value['owner_payloads'][s['subtree_id']]} for s in record['tree']['subtrees']}:raise ValueError('complete owner view differs')
        result.update(status='passed',native_calls=native_calls,remaining_linear_costs=['actual writer wholecanonical copies/allownerclones','full record/namespace audit and tables','legacy page/trie fulloracle/metadata','global routing ownerfrontier'])
    except BaseException as error:result['error']=repr(error);raise
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        result.update(seconds=time.monotonic()-started,native_calls=native_calls)
        encoded=json.dumps(result,indent=2)+'\n'
        if len(encoded.encode())>8*1024*1024:raise ValueError('report exceeds observed budget')
        (out/'result.json').write_text(encoded)
    if result['status']!='passed':raise ValueError('POC replay failed')

if __name__=='__main__':main()
