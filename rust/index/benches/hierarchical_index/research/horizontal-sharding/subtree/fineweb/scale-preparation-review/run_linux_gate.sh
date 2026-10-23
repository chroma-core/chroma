#!/bin/bash
set -euo pipefail
ROOT=/mnt/spann/prep100m-linux-gate-v3
test ! -e "$ROOT"
mkdir -p "$ROOT"/{provenance,runtime/source,runtime/tmp,control}
cp /mnt/spann/prep100m-frozen-producer-v3.tar.gz "$ROOT/provenance/source.tar.gz"
cp /mnt/spann/prep100m-runtime-v3.lock "$ROOT/provenance/runtime.lock"
trap 'status=$?; printf "%s\n" "$status" > "$ROOT/control/exit-status.txt"' EXIT
python3 - "$ROOT" <<'PY'
from pathlib import Path
import tarfile,hashlib,sys
root=Path(sys.argv[1]);p=root/'provenance/source.tar.gz'
assert hashlib.sha256(p.read_bytes()).hexdigest()=='89e0b1a21413d26686df0aac86cc6a95c4914d62d56755ae012cda50afa6bb66'
with tarfile.open(p) as t:
 for m in t:
  rel=Path(m.name);assert m.isfile() and not rel.is_absolute() and '..' not in rel.parts
  target=root/'runtime/source'/rel;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(t.extractfile(m).read())
PY
export TMPDIR="$ROOT/runtime/tmp" PIP_CACHE_DIR="$ROOT/runtime/package-cache" PYTHONNOUSERSITE=1 OPENBLAS_NUM_THREADS=2 OMP_NUM_THREADS=2
unset PYTHONPATH PYTHONOPTIMIZE
python3.12 -m venv --copies "$ROOT/runtime/venv"
timeout 600 "$ROOT/runtime/venv/bin/python" -m pip install --only-binary=:all: --require-hashes -r "$ROOT/provenance/runtime.lock" --report "$ROOT/provenance/pip-report.json" > "$ROOT/control/pip-install.log" 2>&1
"$ROOT/runtime/venv/bin/python" -m pip freeze --all > "$ROOT/provenance/pip-freeze.txt"
"$ROOT/runtime/venv/bin/python" - "$ROOT" <<'PY'
from pathlib import Path
import json,sys,hashlib,platform
root=Path(sys.argv[1]);alias=root/'runtime/venv/lib64';removed=[]
if alias.is_symlink():
 assert alias.resolve()==(root/'runtime/venv/lib').resolve();removed.append(str(alias));alias.unlink()
links=[str(p) for p in root.rglob('*') if p.is_symlink()];assert not links,links[:20]
manifest=json.loads((root/'runtime/source/full-source-manifest.json').read_text())
for name,expected in manifest['files'].items():assert hashlib.sha256((root/'runtime/source'/name).read_bytes()).hexdigest()==expected
(root/'provenance/runtime-proof.json').write_text(json.dumps(dict(python=sys.version,executable=sys.executable,platform=platform.platform(),removed_verified_standard_alias=removed,remaining_symlinks=links,source_files_verified=len(manifest['files'])),indent=2))
PY
cd "$ROOT/runtime/source"
timeout 180 "$ROOT/runtime/venv/bin/python" -m unittest tools.hierarchical_sharding.test_fineweb_stage_profile tools.hierarchical_sharding.test_fineweb_scale_job tools.hierarchical_sharding.test_fineweb_scale_archive tools.hierarchical_sharding.test_fineweb_scale_prepare tools.hierarchical_sharding.test_fineweb_scale_runtime tools.hierarchical_sharding.test_fineweb_scale_pipeline tools.hierarchical_sharding.test_fineweb_scale_handoff tools.hierarchical_sharding.test_fineweb_scale_pilot tools.hierarchical_sharding.test_fineweb_parallel tools.hierarchical_sharding.test_fineweb_stream -v > "$ROOT/control/cpu-tests.log" 2>&1
timeout 90 "$ROOT/runtime/venv/bin/python" -m unittest tools.hierarchical_sharding.test_resource_guard -v > "$ROOT/control/guard-tests.log" 2>&1
"$ROOT/runtime/venv/bin/python" - "$ROOT" <<'PY'
from pathlib import Path
import json,sys
from tools.hierarchical_sharding.resource_guard import inventory
root=Path(sys.argv[1]);scopes=[dict(path=str(root),role='entire-copied-runtime-source-cache-fixtures-control')];r=inventory(scopes,1000000);(root/'control/pre-workflow-inventory.json').write_text(json.dumps(r,indent=2))
config=dict(abort_plan='verified-process-groups-no-detach',commands=[[str(root/'runtime/venv/bin/python'),'-m','unittest','tools.hierarchical_sharding.test_fineweb_scale_prepare.FullMembershipTests.test_complete_verified_workflow_on_bounded_fixture_without_network','tools.hierarchical_sharding.test_fineweb_scale_archive.ArchiveTests.test_all_logical_attempts_have_full_version_pinned_restore','tools.hierarchical_sharding.test_fineweb_scale_handoff','-v']],concurrency=1,sample_interval_s=.2,termination_grace_s=2,timeout_s=120,scopes=scopes,limits=dict(scope_bytes_cap=4*1024**3,scope_stop_margin_bytes=512*1024**2,disk_free_floor_bytes=2*1024**3,disk_stop_margin_bytes=1024**3,rss_cap_bytes=1024**3,rss_stop_margin_bytes=128*1024**2));(root/'control/guard.json').write_text(json.dumps(config,indent=2))
PY
timeout 180 "$ROOT/runtime/venv/bin/python" -m tools.hierarchical_sharding.resource_guard --config "$ROOT/control/guard.json" --output "$ROOT/control/guard-output" > "$ROOT/control/guard-workflow.log" 2>&1
"$ROOT/runtime/venv/bin/python" - "$ROOT" <<'PY'
from pathlib import Path
import json,sys,time,hashlib
from tools.hierarchical_sharding.resource_guard import inventory
root=Path(sys.argv[1]);report=json.loads((root/'control/guard-output/report.json').read_text());assert report['status']=='passed' and all(o['status']=='ok' and not o.get('unreconciled_children') for o in report['outcomes']);scopes=[dict(path=str(root),role='entire-copied-runtime-source-cache-fixtures-control')];final=inventory(scopes,1000000);assert not [p for p in root.rglob('*') if p.is_symlink()]
(root/'control/result.json').write_text(json.dumps(dict(status='passed',finished_unix=time.time(),guard_report_sha256=hashlib.sha256((root/'control/guard-output/report.json').read_bytes()).hexdigest(),runtime=json.loads((root/'provenance/runtime-proof.json').read_text()),final_inventory=final,scope='Installed hash-pinned CPython3.12 copied-venv; all CPU and real Linux process-cleanup tests; actual21-row preparation/reference and version-pinned archive/handoff fixtures run under a strict whole-root guard. The archive unit fixture uses an in-memory store; live large-object SDK/role restoration is separately proven by the preserved real10M archive.'),indent=2));print(json.dumps(dict(status='passed',files=final['inventory_entries'],logical_bytes=final['total_unique_inode_bytes'])))
PY
