"""Bounded local fixture recipe; no network, registration service or local CAS."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

BASE=Path(__file__).resolve().parent
REPO=Path('/private/tmp/chroma-hierarchical-spann-sharding')
BINARY=BASE/'exporter-runtime-binary'
SHA='59fe02aebcf65f1c1319066d4ab18f02e7d405df3d3c65286d616e1d1b2b5580'
assert hashlib.sha256(BINARY.read_bytes()).hexdigest()==SHA
manifest=json.loads((BASE/'manifest.json').read_text())
assert {n:hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in manifest['files']}==manifest['files']
assert __import__('shutil').disk_usage(BASE).free>=20*1024**3
root=BASE/'runtime'
root.mkdir()
sys.path.insert(0,str(REPO))
from tools.hierarchical_sharding.test_compactor_namespace_native import prepare_fixture
prepare_fixture(root/'first',BASE.parent/'streaming-postings-v1/runtime/streaming-full-query/current.json')
env=dict(os.environ,PYTHONPATH=str(REPO),PYTHONPYCACHEPREFIX='/tmp/spann-pycache',SPANN_NAMESPACE_INPUT=str(root/'first/record-input.json'),SPANN_NAMESPACE_OUTPUT=str(root/'first/record-export'))
run=subprocess.run([str(BINARY),'--ignored','--exact','export_actual_namespace_append','--nocapture'],env=env,capture_output=True,timeout=30)
(BASE/'genesis-export.stdout').write_bytes(run.stdout)
(BASE/'genesis-export.stderr').write_bytes(run.stderr)
assert run.returncode==0,run.stderr.decode()
proof=json.loads((root/'first/record-export/export.receipt.json').read_text())
assert proof['runtime_binary']['sha256']==SHA
for name,sha in proof['producer_sources'].items():assert sha==manifest['files'][name]
for name in ('export','input','runtime_binary'):
    artifact=proof[name];payload=Path(artifact['path']).read_bytes()
    assert len(payload)==artifact['bytes'] and hashlib.sha256(payload).hexdigest()==artifact['sha256']
env.update(SPANN_NAMESPACE_REPEATED=str(root),SPANN_NAMESPACE_PIPELINE=str(root/'first'),SPANN_NAMESPACE_RUNTIME_SHA=SHA,SPANN_WRITE_PLANNER_BINARY='/private/tmp/spann-native-singlecopy-v5/hierarchical_spann_compactor',SPANN_DYNAMIC_BINARY='/private/tmp/spann-native-745c-reviewed/hierarchical_spann_shard')
method='tools.hierarchical_sharding.test_compactor_namespace_native.NativeRecordFlushFault.test_partial_root_write_fails_private_then_fresh_parent_recovers'
# Each exporter has a 30s timeout; writer and read timeouts remain in the
# admitted producer. There is no claimed hard aggregate wall/RSS limit here.
run=subprocess.run([sys.executable,'-m','unittest','-v',method],env=env,capture_output=True)
(BASE/'fixture.stdout').write_bytes(run.stdout)
(BASE/'fixture.stderr').write_bytes(run.stderr)
end={n:hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in manifest['files']}
receipt=dict(exit_code=run.returncode,source_end_hashes=end,source_unchanged=end==manifest['files'],runtime_binary_sha256=SHA,fixture_files_bytes=sum(p.stat().st_size for p in root.rglob('*') if p.is_file()),fixture_stdout_sha256=hashlib.sha256(run.stdout).hexdigest(),fixture_stderr_sha256=hashlib.sha256(run.stderr).hexdigest(),scope='actual private record flush error and fresh-parent recovery; local fake authority')
(BASE/'runtime-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert receipt['source_unchanged']
assert run.returncode==0,run.stderr.decode()
assert receipt['fixture_files_bytes']<=100*1024**2,'observed fixture output exceeded declared budget'
print(json.dumps({k:v for k,v in receipt.items() if k!='source_end_hashes'},indent=2))
