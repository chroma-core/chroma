#!/bin/bash
set -euo pipefail
# Infrastructure owner verifies Cops-managed sandbox account/instance before restart.
# Stage the immutable materialized bundle on root EBS; no bulk artifacts belong there.
STAGE=${1:?materialized bundle directory required}
EVIDENCE=/var/lib/spann-prep100m-evidence
test ! -e "$EVIDENCE"
mkdir -p "$EVIDENCE"
trap 'status=$?; printf "%s\n" "$status" > "$EVIDENCE/bootstrap-exit.txt"' EXIT
TOKEN=$(curl --noproxy '*' --max-time 3 -fsS -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token)
curl --noproxy '*' --max-time 3 -fsS -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/document > "$EVIDENCE/instance.json"
python3 - "$STAGE/job.json" "$EVIDENCE/instance.json" <<'PY'
import json,sys,time
job=json.load(open(sys.argv[1]));actual=json.load(open(sys.argv[2]))
for key,field in [('instanceId','expected_instance_id'),('accountId','expected_account'),('region','region'),('instanceType','instance_type')]:
 if actual[key]!=job[field]:raise SystemExit('sandbox instance identity mismatch: '+key)
boot_epoch=time.time()-float(open('/proc/uptime').read().split()[0])
if abs(job['started_epoch']-boot_epoch)>2:raise SystemExit('job clock is not the actual node boot clock')
if job['stop_epoch']-job['started_epoch']!=21600 or not job['started_epoch']<=time.time()<job['stop_epoch']-1800:raise SystemExit('fixed node clock expired or changed')
PY
# Existing benchmark shutdown timers require infrastructure-owner reconciliation.
systemctl list-timers --all --no-pager > "$EVIDENCE/existing-timers.txt"
if systemctl list-timers --no-legend --no-pager | grep -E 'spann.*\.timer' > "$EVIDENCE/conflicting-timers.txt"; then
 echo 'Existing active benchmark timer requires explicit reconciliation' >&2
 exit 1
fi
STOP=$(python3 -c 'import json,sys,datetime; print(datetime.datetime.fromtimestamp(json.load(open(sys.argv[1]))["stop_epoch"],datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"))' "$STAGE/job.json")
cat > "$EVIDENCE/hard-stop.sh" <<'STOP_SCRIPT'
#!/bin/bash
set +e
timeout 20 python3 - <<'PY'
from pathlib import Path
import shutil,json,time
out=Path('/var/lib/spann-prep100m-evidence/final');out.mkdir(exist_ok=True);used=0;copied=[]
root=Path('/mnt/spann/prep100m-v1')
for folder in ['reports','archive','control']:
 for p in (root/folder).rglob('*'):
  if p.is_symlink() or not p.is_file() or p.suffix not in ('.json','.jsonl','.log'):continue
  size=p.stat().st_size
  if size>8*1024**2 or used+size>64*1024**2:continue
  target=out/p.relative_to(root);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target);used+=size;copied.append(str(p))
p=root/'pipeline.json'
if p.exists() and p.stat().st_size<=8*1024**2 and used+p.stat().st_size<=64*1024**2:shutil.copyfile(p,out/'pipeline.json')
(out/'receipt.json').write_text(json.dumps(dict(epoch=time.time(),bytes=used,copied=copied)))
PY
timeout 5 sync
shutdown -h now
STOP_SCRIPT
chmod 700 "$EVIDENCE/hard-stop.sh"
systemd-run --unit=spann-prep100m-hard-stop --on-calendar="$STOP" --timer-property=AccuracySec=1s "$EVIDENCE/hard-stop.sh"
systemctl show spann-prep100m-hard-stop.timer > "$EVIDENCE/hard-stop-timer.txt"
# A restarted instance has no reusable NVMe contents. Format only a blank instance-store device.
if ! mountpoint -q /mnt/spann; then
 mapfile -t disks < <(lsblk -dnpo NAME,MODEL | awk '/Amazon EC2 NVMe Instance Storage/ {print $1}')
 ((${#disks[@]}==1))
 disk=${disks[0]}
 test -z "$(lsblk -nro MOUNTPOINT "$disk" | tr -d '[:space:]')"
 test -z "$(wipefs -n "$disk")"
 mkfs.ext4 -F "$disk"
 mkdir -p /mnt/spann
 mount "$disk" /mnt/spann
fi
ROOT=/mnt/spann/prep100m-v1
test ! -e "$ROOT"
mkdir -p "$ROOT"/{provenance,runtime/source,control,pilot}
cp -a "$STAGE/provenance/." "$ROOT/provenance/"
cp "$STAGE/job.json" "$ROOT/job.json"
python3 - "$ROOT" <<'PY'
from pathlib import Path
import json,hashlib,sys,tarfile
root=Path(sys.argv[1]);job=json.loads((root/'job.json').read_text())
if root.stat().st_dev==Path('/').stat().st_dev:raise SystemExit('bulk root is on root EBS')
for key in ['source_manifest','source_plan','read_gate','runtime_lock','source_archive']:
 d=job[key];b=Path(d['path']).read_bytes()
 if len(b)!=d['bytes'] or hashlib.sha256(b).hexdigest()!=d['sha256']:raise SystemExit('staged admission artifact differs')
with tarfile.open(root/'provenance/source.tar.gz') as t:
 for m in t:
  p=Path(m.name)
  if not m.isfile() or p.is_absolute() or '..' in p.parts:raise SystemExit('unsafe source member')
  target=root/'runtime/source'/p;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(t.extractfile(m).read())
PY
export DEBIAN_FRONTEND=noninteractive
timeout 180 apt-get update > "$EVIDENCE/apt-update.log" 2>&1
timeout 180 apt-get install -y python3.12-venv > "$EVIDENCE/apt-runtime.log" 2>&1
python3.12 -m venv --copies "$ROOT/runtime/venv"
unset AWS_PROFILE AWS_DEFAULT_PROFILE AWS_CONFIG_FILE AWS_SHARED_CREDENTIALS_FILE
mkdir -p "$ROOT/runtime/tmp"
export TMPDIR="$ROOT/runtime/tmp"
export PIP_CACHE_DIR="$ROOT/runtime/package-cache"
timeout 600 "$ROOT/runtime/venv/bin/python" -m pip install --only-binary=:all: --require-hashes -r "$ROOT/provenance/runtime.lock" --report "$EVIDENCE/pip-report.json" > "$EVIDENCE/pip-install.log" 2>&1
"$ROOT/runtime/venv/bin/python" -m pip freeze --all > "$EVIDENCE/pip-freeze.txt"
cp "$EVIDENCE/instance.json" "$EVIDENCE/pip-report.json" "$EVIDENCE/pip-freeze.txt" "$EVIDENCE/hard-stop-timer.txt" "$ROOT/provenance/"
# Inventory every owned runtime file under the unchanged strict guard contract.
"$ROOT/runtime/venv/bin/python" - "$ROOT/runtime" <<'PY_RUNTIME'
from pathlib import Path
import sys
runtime=Path(sys.argv[1]);alias=runtime/'venv/lib64'
if alias.is_symlink():
 if alias.resolve()!=(runtime/'venv/lib').resolve():raise SystemExit('unexpected venv lib64 target')
 alias.unlink()
links=[str(p) for p in runtime.rglob('*') if p.is_symlink()]
if links:raise SystemExit('runtime symlinks violate strict inventory: '+repr(links[:20]))
PY_RUNTIME
export OPENBLAS_NUM_THREADS=8 OMP_NUM_THREADS=8
unset PYTHONOPTIMIZE
cd "$ROOT/runtime/source"
timeout 120 "$ROOT/runtime/venv/bin/python" -m unittest tools.hierarchical_sharding.test_fineweb_stage_profile tools.hierarchical_sharding.test_fineweb_scale_job tools.hierarchical_sharding.test_fineweb_scale_archive tools.hierarchical_sharding.test_fineweb_scale_prepare tools.hierarchical_sharding.test_fineweb_scale_runtime tools.hierarchical_sharding.test_fineweb_scale_pipeline tools.hierarchical_sharding.test_fineweb_scale_handoff tools.hierarchical_sharding.test_fineweb_scale_pilot tools.hierarchical_sharding.test_fineweb_parallel tools.hierarchical_sharding.test_fineweb_stream -v > "$EVIDENCE/cpu-gates.log" 2>&1
"$ROOT/runtime/venv/bin/python" - "$ROOT" <<'PY'
import json,time,sys
from pathlib import Path
from tools.hierarchical_sharding.tree_manifest import canonical_bytes
root=Path(sys.argv[1]);job=json.loads((root/'job.json').read_text());remaining=int(job['stop_epoch']-time.time()-1800)
if remaining<=0:raise SystemExit('no work time remains')
guard=dict(abort_plan='verified-process-groups-no-detach',commands=[[str(root/'runtime/venv/bin/python'),'-m','tools.hierarchical_sharding.fineweb_scale_pipeline','--job',str(root/'job.json')]],concurrency=1,sample_interval_s=1,timeout_s=remaining,termination_grace_s=2,scopes=[dict(path=str(root),role='all-private-data-attempts-runtime-archive-restore-control')],limits=dict(scope_bytes_cap=2*1024**4,scope_stop_margin_bytes=256*1024**3,disk_free_floor_bytes=512*1024**3,disk_stop_margin_bytes=128*1024**3,rss_cap_bytes=32*1024**3,rss_stop_margin_bytes=4*1024**3))
(root/'control/guard.json').write_bytes(canonical_bytes(guard))
PY
"$ROOT/runtime/venv/bin/python" -m tools.hierarchical_sharding.resource_guard --config "$ROOT/control/guard.json" --output "$ROOT/control/guard-output" > "$EVIDENCE/guard.log" 2>&1

"$ROOT/runtime/venv/bin/python" -m tools.hierarchical_sharding.fineweb_scale_handoff --job "$ROOT/job.json" > "$EVIDENCE/handoff.log" 2>&1
