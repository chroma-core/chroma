"""Paired fixed-parent append experiment; full audit is an explicit prerequisite."""
import argparse
import hashlib
import json
import signal
import struct
import subprocess
import time
from pathlib import Path
from .measure_rerank_pins import inventory
from .tree_manifest import checked_artifact, content_hash, load_model


def main():
    parser=argparse.ArgumentParser()
    for key in ('fixture','output','binary','manifest','expected-inventory','bootstrap'):parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--expected-binary-sha256',required=True)
    args=parser.parse_args();root=args.fixture.resolve();out=args.output.resolve();repo=Path(__file__).resolve().parents[2]
    sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
    if root==out or root in out.parents or out in root.parents:raise ValueError('overlapping output')
    pins=json.loads(args.manifest.read_bytes())['files'];before=inventory(root)
    if before!=json.loads(args.expected_inventory.read_bytes()) or sha(args.binary)!=args.expected_binary_sha256 or {p:sha(repo/p) for p in pins}!=pins:raise ValueError('source/input/binary differs')
    bootstrap=json.loads(args.bootstrap.read_bytes())
    if bootstrap['status']!='passed':raise ValueError('separate full bootstrap required')
    parent=json.loads((root/'parent.json').read_bytes());receipt=json.loads((root/'receipt.json').read_bytes());reference=json.loads(checked_artifact(receipt['append_bundle'],'.').read_bytes());expected=json.loads(checked_artifact(reference['writer_execution'],'.').read_bytes())
    # Exact prior full audit is experiment evidence, never a provider immutability guarantee.
    bootstrap_value=bootstrap['phases']['bootstrap-parent96']['value']
    if bootstrap_value['tree_sha256']!=parent['tree_sha256']:raise ValueError('bootstrap tree differs')
    export=json.loads(checked_artifact(reference['materialized_source_binding']['export'],'.').read_bytes())
    rows=export['captured']['rows'];tail=b''.join(struct.pack('<'+'I'*len(r['embedding_bits']),*r['embedding_bits']) for r in rows)
    if [r['global_id'] for r in rows]!=[96,97] or [r['record_offset_id'] for r in rows]!=[97,98] or len(tail)!=256:raise ValueError('retained actual materializer differs')
    old=parent['canonical_vectors'];grown=reference['deployment']['canonical_vectors']
    with Path(grown['path']).open('rb') as f:f.seek(12288);actual=f.read(256)
    if actual!=tail:raise ValueError('actual vector bits differ')
    view=dict(assumption='fixed-immutable-input-poc',dimension=32,rows=98,spans=[dict(path=old['path'],global_start=0,rows=96,source_offset_bytes=0,bytes=12288,sha256=old['sha256']),dict(path=grown['path'],global_start=96,rows=2,source_offset_bytes=12288,bytes=256,sha256=hashlib.sha256(tail).hexdigest())])
    model=load_model(parent['tree']);owners=[dict(subtree_id=s['subtree_id'],checkpoint_dir=parent['owners'][s['subtree_id']]['checkpoint_dir'],generation=s['generation'],manifest_sha256=s['snapshot']['sha256'],centroid=model['centroids'][s['centroid_index']]) for s in sorted(parent['tree']['subtrees'],key=lambda s:s['subtree_id'])]
    out.mkdir(parents=True,exist_ok=False);(out/'before.json').write_text(json.dumps(before,indent=2)+'\n')
    result=dict(status='failed',assumption='exact retained saved inputs remain immutable for the experiment',bootstrap=dict(path=str(args.bootstrap.resolve()),sha256=sha(args.bootstrap),scope='prior full audit, separately charged; count state does not prove old coverage'),modes={});started=time.monotonic()
    def stop(*_):raise TimeoutError('90s experiment work deadline')
    previous=signal.signal(signal.SIGALRM,stop);signal.alarm(90)
    try:
        for mode in ('full-prefix','counted-parent'):
            directory=out/mode;directory.mkdir();spec=dict(assumption='fixed-immutable-input-poc',counted_parent=mode=='counted-parent',parent_sha256=content_hash(parent),source_id=parent['tree']['source']['source_id'],dimension=32,metric=parent['tree']['metric'],old_count=96,view=view,owners=owners,output=str(directory/'writer'))
            spec_path=directory/'spec.json';spec_path.write_text(json.dumps(spec)+'\n')
            with (directory/'writer.stdout').open('xb') as stdout,(directory/'writer.stderr').open('xb') as stderr:process=subprocess.run([str(args.binary.resolve()),'poc-lazy-global-writes','--spec',str(spec_path)],stdout=stdout,stderr=stderr,timeout=30)
            if process.returncode or (directory/'writer.stdout').stat().st_size>1024*1024:raise ValueError('actual writer failed: '+mode)
            reply=json.loads((directory/'writer.stdout').read_bytes())
            if reply['routes']!=expected['routes'] or reply['copies']['a']!={'copied_bytes':0,'copied_files':0} or len(reply['saved'])!=1:raise ValueError('route or selected-owner copy differs')
            # Expand unchanged ownership only in the separately accounted controller oracle.
            groups={r['subtree_id']:r.get('ids') for r in reply['owners']};oracle={r['subtree_id']:r['ids'] for r in expected['owners']}
            if mode=='counted-parent':
                a=next(r for r in reply['owners'] if r['subtree_id']=='a')
                if a['count']!=48 or a['unchanged_map'] is None or groups['a'] is not None:raise ValueError('unchanged map was materialized')
                groups['a']=oracle['a']
            if groups!=oracle or sorted(i for ids in groups.values() for i in ids)!=list(range(98)):raise ValueError('complete ownership differs')
            saved=reply['saved'][0];checkpoint=Path(saved['checkpoint_dir']);manifest_path=checkpoint/'manifest-2.json';manifest=json.loads(manifest_path.read_bytes());mapping=checkpoint/manifest['id_mapping']['path']
            if [v[0] for v in struct.iter_unpack('<Q',mapping.read_bytes())]!=oracle['b']:raise ValueError('saved map differs')
            selected=[(r['leaf_id'],r['local_id']) for r in reply['routes']]
            audit=dict(assumption='fixed-immutable-input-poc',checkpoint_dir=str(checkpoint),generation=2,manifest_sha256=sha(manifest_path),map=str(mapping),map_sha256=sha(mapping),view=view,selected=selected,subtree_id='b',source_id=spec['source_id'],metric=spec['metric'],routing_boundary_hash=manifest['routing_boundary_hash'],root_ref=manifest['root_ref'])
            audit_path=directory/'audit.json';audit_path.write_text(json.dumps(audit)+'\n')
            with (directory/'audit.stdout').open('xb') as stdout,(directory/'audit.stderr').open('xb') as stderr:auditor=subprocess.run([str(args.binary.resolve()),'poc-audit-snapshot','--spec',str(audit_path)],stdout=stdout,stderr=stderr,timeout=10)
            if auditor.returncode or (directory/'audit.stdout').stat().st_size>1024*1024:raise ValueError('saved audit failed')
            checked=json.loads((directory/'audit.stdout').read_bytes())
            if checked['global_ids']!=oracle['b'] or checked['selected']!=[list(v) for v in selected] or checked['coverage']['duplicate_current_entries']!=0:raise ValueError('saved version/posting/vector/leaf proof differs')
            result['modes'][mode]=dict(write=reply,saved_audit=checked,exitcodes=[process.returncode,auditor.returncode])
        baseline=result['modes']['full-prefix']['write'];counted=result['modes']['counted-parent']['write']
        if baseline['coordination']['old_version_probes']!=193 or counted['coordination']['old_version_probes']!=0:raise ValueError('old-version work differs')
        if baseline['parent_map_manifest_reads']['a']['map_bytes']!=384 or counted['parent_map_manifest_reads']['a']['map_bytes']!=0 or counted['parent_map_manifest_reads']['b']['map_bytes']!=384:raise ValueError('map work differs')
        result.update(status='passed',remaining_costs=['all-owner node/centroid metadata opens','per-row topology width recomputation and all-owner global routing','selected-leaf versions and full selected-owner map/vector audit','record namespace and legacy complete oracle outside experiment'],scope='work elimination and exact output parity, not latency/physical I/O/RSS/100B throughput')
    except BaseException as error:result['error']=repr(error);raise
    finally:
        signal.alarm(0);signal.signal(signal.SIGALRM,previous)
        try:
            after=inventory(root);(out/'after.json').write_text(json.dumps(after,indent=2)+'\n');result['inventory_unchanged']=after==before;result['end_source_hashes']={p:sha(repo/p) for p in pins}
            if after!=before or result['end_source_hashes']!=pins:result['status']='failed-reconciliation'
        except BaseException as error:result.update(status='failed-reconciliation',reconciliation_error=repr(error))
        result['seconds']=time.monotonic()-started;files=[p for p in out.rglob('*') if p.is_file()];result['output_bytes']=sum(p.stat().st_size for p in files);result['output_files']=len(files)
        if result['output_bytes']>16*1024*1024 or len(files)>4096:result['status']='failed-observed-output-budget'
        data=json.dumps(result,indent=2)+'\n'
        if len(data.encode())>8*1024*1024:raise ValueError('report budget exceeded')
        (out/'result.json').write_text(data)
    if result['status']!='passed':raise ValueError('experiment did not reconcile')

if __name__=='__main__':main()
