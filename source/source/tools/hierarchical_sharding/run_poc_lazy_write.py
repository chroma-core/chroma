"""One retained-data selected-leaf write with spans and selected-owner copies."""
import argparse
import hashlib
import json
import signal
import struct
import subprocess
import time
from pathlib import Path
from .measure_rerank_pins import inventory
from .tree_manifest import checked_artifact,content_hash,load_model,artifact


def copied(source,destination,offset=0,length=None):
    destination.parent.mkdir(parents=True,exist_ok=True);total=0
    with Path(source).open('rb') as src,destination.open('xb') as out:
        src.seek(offset)
        while length is None or total<length:
            data=src.read(65536 if length is None else min(65536,length-total))
            if not data:break
            written=out.write(data)
            if written!=len(data):raise ValueError('short copy')
            total+=written
    if length is not None and total!=length:raise ValueError('copy shape differs')
    return total


def baseline_copies(parent,tail_path,output):
    row_bytes=parent['tree']['dimension']*4;old_count=parent['tree']['source']['row_count']
    first=output/'canonical.f32';old_bytes=copied(parent['canonical_vectors']['path'],first)
    with Path(tail_path).open('rb') as source,first.open('ab') as destination:
        source.seek(old_count*row_bytes);tail=source.read(2*row_bytes);assert len(tail)==2*row_bytes;assert destination.write(tail)==len(tail)
    second=output/'private-writers/canonical.f32';second_bytes=copied(first,second)
    suffix_bytes=copied(second,output/'private-writers/append.f32',old_count*row_bytes,2*row_bytes)
    owners={}
    for sid,owner in parent['owners'].items():
        source=Path(owner['checkpoint_dir']);counts=dict(copied_bytes=0,copied_files=0)
        for path in sorted(source.rglob('*')):
            if path.is_dir():continue
            if path.is_symlink() or not path.is_file():raise ValueError('regular baseline input required')
            counts['copied_bytes']+=copied(path,output/('index-'+sid)/path.relative_to(source));counts['copied_files']+=1
        owners[sid]=counts
    return dict(canonical_materialization_bytes=old_bytes+len(tail)+second_bytes,append_file_bytes=suffix_bytes,canonical_returned_bytes=old_bytes+len(tail)+second_bytes+suffix_bytes,owners=owners,scope='actual copy-only baseline reproduces eager copies; no baseline route/save replay; logical bytes, not physical I/O')


def assert_complete(reply,expected,parent):
    if reply['status']!='saved-poc-lazy-global-writes' or reply['routes']!=expected['routes'] or reply['owners']!=expected['owners']:raise ValueError('actual global route/ownership differs from retained independent writer')
    if [r['id'] for r in reply['routes']]!=[96,97] or any(r['subtree_id']!='b' for r in reply['routes']):raise ValueError('cross-boundary single destination differs')
    groups={r['subtree_id']:r['ids'] for r in reply['owners']};flat=[i for ids in groups.values() for i in ids]
    if sorted(flat)!=list(range(98)) or len(flat)!=len(set(flat)):raise ValueError('global ownership differs')
    if reply['copies']['a']!={'copied_bytes':0,'copied_files':0} or len(reply['saved'])!=1:raise ValueError('unchanged owner was prepared')
    if reply['canonical_materialization_bytes']!=0:raise ValueError('POC materializes canonical corpus')
    return groups


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','output','binary','manifest','expected-inventory'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True);a=parser.parse_args();root=a.fixture.resolve();out=a.output.resolve();repo=Path(__file__).resolve().parents[2];sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    if root==out or root in out.parents or out in root.parents:raise ValueError('output overlaps fixture')
    pins=json.loads(a.manifest.read_bytes())['files'];before=inventory(root)
    if before!=json.loads(a.expected_inventory.read_bytes()) or sha(a.binary)!=a.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('input/source/binary admission differs')
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    parent=json.loads((root/'parent.json').read_bytes());receipt=json.loads((root/'receipt.json').read_bytes());reference=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes());expected=json.loads(checked_artifact(reference['writer_execution'],'.').read_bytes());export=json.loads(checked_artifact(reference['materialized_source_binding']['export'],'.').read_bytes())
    rows=export['captured']['rows']
    if [r['global_id'] for r in rows]!=[96,97] or [r['record_offset_id'] for r in rows]!=[97,98]:raise ValueError('actual retained materializer IDs differ')
    tail=b''.join(struct.pack('<'+'I'*len(r['embedding_bits']),*r['embedding_bits']) for r in rows)
    old=parent['canonical_vectors'];grown=reference['deployment']['canonical_vectors'];row_bytes=32*4
    with Path(grown['path']).open('rb') as source:source.seek(96*row_bytes);actual=source.read(2*row_bytes)
    if actual!=tail or len(tail)!=256:raise ValueError('actual materialized vector bits differ')
    view=dict(assumption='fixed-immutable-input-poc',dimension=32,rows=98,spans=[dict(path=old['path'],global_start=0,rows=96,source_offset_bytes=0,bytes=96*row_bytes,sha256=old['sha256']),dict(path=grown['path'],global_start=96,rows=2,source_offset_bytes=96*row_bytes,bytes=256,sha256=hashlib.sha256(tail).hexdigest())])
    model=load_model(parent['tree']);owners=[dict(subtree_id=s['subtree_id'],checkpoint_dir=parent['owners'][s['subtree_id']]['checkpoint_dir'],generation=s['generation'],manifest_sha256=s['snapshot']['sha256'],centroid=model['centroids'][s['centroid_index']]) for s in sorted(parent['tree']['subtrees'],key=lambda s:s['subtree_id'])]
    spec=dict(assumption='fixed-immutable-input-poc',parent_sha256=content_hash(parent),source_id=parent['tree']['source']['source_id'],dimension=32,metric=parent['tree']['metric'],old_count=96,view=view,owners=owners,output=str(out/'actual-writer'))
    spec_path=out/'writer-spec.json';spec_path.write_text(json.dumps(spec)+'\n');result=dict(status='failed',setup=dict(materialized_export=reference['materialized_source_binding']['export'],tail_read_bytes=256,source_inventory_bytes=sum(x['bytes'] for x in before.values())),assumption='retained saved inputs immutable; POC only');started=time.monotonic()
    def stop(*_):raise TimeoutError('POC writer work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(90)
    try:
        result['baseline_copies']=baseline_copies(parent,grown['path'],out/'copy-only-baseline')
        with (out/'writer.stdout').open('xb') as stdout,(out/'writer.stderr').open('xb') as stderr:process=subprocess.run([str(a.binary.resolve()),'poc-lazy-global-writes','--spec',str(spec_path)],stdout=stdout,stderr=stderr,timeout=30)
        if process.returncode or (out/'writer.stdout').stat().st_size>1024*1024:raise ValueError('actual selected writer failed')
        reply=json.loads((out/'writer.stdout').read_bytes());groups=assert_complete(reply,expected,parent)
        if (out/'actual-writer/index-a').exists():raise ValueError('unchanged checkpoint copy exists')
        if reply['copies']['b']!=result['baseline_copies']['owners']['b']:raise ValueError('selected copy counters differ from actual baseline')
        saved=reply['saved'][0];directory=Path(saved['checkpoint_dir']);manifest=json.loads((directory/'manifest-2.json').read_bytes());mapping=directory/manifest['id_mapping']['path']
        if [i[0] for i in struct.iter_unpack('<Q',mapping.read_bytes())]!=groups['b']:raise ValueError('persisted map differs')
        selected=[(r['leaf_id'],r['local_id']) for r in reply['routes']]
        audit=dict(assumption='fixed-immutable-input-poc',checkpoint_dir=str(directory),generation=2,manifest_sha256=sha(directory/'manifest-2.json'),map=str(mapping),map_sha256=sha(mapping),view=view,selected=selected,subtree_id='b',source_id=spec['source_id'],metric=spec['metric'],routing_boundary_hash=manifest['routing_boundary_hash'],root_ref=manifest['root_ref'])
        audit_path=out/'saved-audit.json';audit_path.write_text(json.dumps(audit)+'\n')
        with (out/'audit.stdout').open('xb') as stdout,(out/'audit.stderr').open('xb') as stderr:auditor=subprocess.run([str(a.binary.resolve()),'poc-audit-snapshot','--spec',str(audit_path)],stdout=stdout,stderr=stderr,timeout=10)
        if auditor.returncode or (out/'audit.stdout').stat().st_size>1024*1024:raise ValueError('actual saved-owner audit failed')
        checked=json.loads((out/'audit.stdout').read_bytes())
        if checked['global_ids']!=groups['b'] or checked['selected']!=[list(v) for v in selected] or checked['coverage']['duplicate_current_entries']!=0:raise ValueError('saved versions/postings/bits/memberships differ')
        owner_view={s['subtree_id']:dict(snapshot=s['snapshot'],local_to_global=s['local_to_global'],root_ref=s['root_ref']) for s in parent['tree']['subtrees']};owner_view['b']=dict(snapshot=artifact(directory/'manifest-2.json'),local_to_global=artifact(mapping),root_ref=manifest['root_ref'])
        result.update(status='passed',actual_write=reply,saved_audit=checked,complete_poc_view=dict(parent_sha256=spec['parent_sha256'],source_id=spec['source_id'],rows=98,canonical_view=view,model=parent['tree']['model'],owners=owner_view,scope='complete vector ownership view; record/legacy proposal not rerun'),reference=dict(bundle=receipt['append_bundle'],writer=reference['writer_execution'],alias_premises=receipt['alias_premises']),exitcodes=[process.returncode,auditor.returncode],oracle_costs=dict(retained_report='poc-proportional-validation-v3/runtime-attempt1/observed-comparison.json',scope='separate prior full oracle/bootstrap evidence; not charged as incremental writer work'))
    except BaseException as error:result['error']=repr(error);raise
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        result['seconds']=time.monotonic()-started
        output_files=[p for p in out.rglob('*') if p.is_file()]
        result['output_files']=len(output_files);result['output_bytes']=sum(p.stat().st_size for p in output_files)
        if result['output_files']>4096 or result['output_bytes']>16*1024*1024:result['status']='failed-observed-output-budget'
        data=json.dumps(result,indent=2)+'\n'
        if len(data.encode())>8*1024*1024:raise ValueError('observed report exceeded budget')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('POC writer did not reconcile')

if __name__=='__main__':main()
