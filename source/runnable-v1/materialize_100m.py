"""Create a runnable immutable job only from a passed10M gate and a concrete node clock."""
import argparse,hashlib,json,shutil,tarfile,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
CODE=Path('/private/tmp/chroma-hierarchical-spann-sharding')
SOURCES=['fineweb_scale_prepare.py','fineweb_scale_pilot.py','fineweb_scale_handoff.py','fineweb_scale_pipeline.py','fineweb_scale_archive.py','fineweb_scale_runtime.py','fineweb_scale_job.py','fineweb_parallel.py','fineweb_stream.py','fineweb_prepare.py','fineweb_source_plan.py','spatial_prepare.py','exact_blocked.py','mapped_rows.py','tree_manifest.py','preparation_attempt.py','resource_guard.py','host_supervisor.py','artifact_store.py','benchmark.py']

def materialize(output,read_gate,expected_gate_sha,runtime_lock,started,node_id):
 output=Path(output);output.mkdir(parents=True,exist_ok=False);provenance=output/'provenance';provenance.mkdir()
 gate=Path(read_gate);payload=gate.read_bytes();r=json.loads(payload)
 if hashlib.sha256(payload).hexdigest()!=expected_gate_sha or r.get('status')!='passed' or r.get('corpus_rows')!=10000000 or r.get('allow_100m_preparation') is not True:raise ValueError('passed hash-bound10M gate required')
 if not node_id.startswith('i-') or not 0<=time.time()-started<1800:raise ValueError('actual recently started node identity/clock required')
 remote=Path('/mnt/spann/prep100m-v1');files={}
 def copy(name,source):
  data=Path(source).read_bytes();(provenance/name).write_bytes(data);desc=dict(path=str(remote/'provenance'/name),bytes=len(data),sha256=hashlib.sha256(data).hexdigest());files[name]=desc;return desc
 source=copy('source-manifest.json',ROOT/'source-manifest-v2.json')
 footer=copy('100m-footer-v1.json',Path(__file__).with_name('100m-footer-v1.json'))
 gate_desc=copy('read-gate.json',gate);lock=copy('runtime.lock',runtime_lock)
 pools={}
 for pool in ('calibration','test'):
  pools[pool]=dict(copy(pool+'.jsonl',ROOT/'fineweb-pilot-preparation-v1/queries'/(pool+'.jsonl')),count=100)
 source_root=CODE/'tools/hierarchical_sharding';sources=[]
 for name in SOURCES:
  p=source_root/name;sources.append(dict(path=str(remote/'runtime/source/tools/hierarchical_sharding'/name),bytes=p.stat().st_size,sha256=hashlib.sha256(p.read_bytes()).hexdigest()))
 archive=provenance/'source.tar.gz'
 with tarfile.open(archive,'w:gz') as t:
  for p in sorted(source_root.glob('*.py')):t.add(p,arcname='tools/hierarchical_sharding/'+p.name,recursive=False)
 source_archive=dict(path=str(remote/'provenance/source.tar.gz'),bytes=archive.stat().st_size,sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
 job=dict(format_version=1,run_concurrency_pilot=False,source_archive=source_archive,stage_estimates_s={'ingestion':6800,'whole-prefix-model':60,'routing':1200,'full-ID-and-bitwise-routing-audit':1600,'whole-prefix-sample-and-UUID-spotcheck':250,'exact-calibration':1100,'exact-test':1100,'independent-full100m-reference-check':600,'archive-and-restore':6000},status='prepared_not_started',expected_instance_id=node_id,expected_account='079819354890',region='us-east-1',instance_type='i4i.4xlarge',started_epoch=started,stop_epoch=started+21600,owned_root=str(remote),dataset='Qdrant/FineWeb-10B',revision='3aea0b8d5f98741e343d07f067ce0f49dca1ccd2',count=100000000,dimension=768,metric='ip',source_manifest=source,source_manifest_sha256='58273841961a99dfaa34762c211148f38686777bfa3dd43e8331a53557722398',source_plan=footer,read_gate=gate_desc,runtime_lock=lock,producer_sources=sources,reports=str(remote/'reports'),query_pools=pools,
  ingest=dict(output=str(remote/'ingest'),workers=2,chunk_rows=100000,batch_rows=2048,retries=1,max_source_bytes=384*1024**3,max_output_bytes=512*1024**3,max_wall_s=19800),
  spatial=dict(output=str(remote/'spatial'),construction_epoch='fineweb100m-128slots-v1',children=128,sample_size=65536,seed=1729,iterations=12,batch_rows=4096),
  resources=dict(hard_timeout_s=19800,max_new_logical_output_bytes=2*1024**4,minimum_free_allocated_bytes=640*1024**3,minimum_start_free_allocated_bytes=(640+2048)*1024**3,max_process_rss_bytes=32*1024**3,monitor_interval_s=1),
  artifact_store=dict(bucket='spann-artifacts-079819354890-20261014-ecfcca4d',account='079819354890',role='spann-artifact-ecfcca4d'),
  archive=dict(output=str(remote/'archive'),restore=str(remote/'restore'),scopes=[str(remote/k) for k in ('ingest','spatial','reports','provenance','pilot')],concurrency=4,part_bytes=64*1024**2,maximum_part_count=10000))
 (output/'job.json').write_text(json.dumps(job,indent=2)+'\n');(provenance/'job.json').write_bytes((output/'job.json').read_bytes());(output/'staging.json').write_text(json.dumps(dict(files=files,source_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),note='Stage only after account/instance/NVMe checks. No bootstrap or node start occurs here.'),indent=2)+'\n');return job

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for key in ('output','read-gate','expected-gate-sha','runtime-lock','node-id'):p.add_argument('--'+key,required=True)
 p.add_argument('--started-epoch',required=True,type=float);a=p.parse_args();materialize(a.output,a.read_gate,a.expected_gate_sha,a.runtime_lock,a.started_epoch,a.node_id)
if __name__=='__main__':main()
