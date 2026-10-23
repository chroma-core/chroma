#!/usr/bin/env bash
set -euo pipefail
cd /mnt/spann/source
export CARGO_HOME=/mnt/spann/cargo RUSTUP_HOME=/mnt/spann/rustup CARGO_TARGET_DIR=/mnt/spann/target-reader-memory-v4 TMPDIR=/mnt/spann/tmp CARGO_BUILD_JOBS=16
export PATH="$CARGO_HOME/bin:$PATH"
python3 - <<'PY'
import pathlib,json,tarfile,hashlib
root=pathlib.Path('/mnt/spann/source');out=pathlib.Path('/mnt/spann/results/storage-audit-v1');files=['Cargo.toml','Cargo.lock','rust/index/Cargo.toml','rust/index/benches/hierarchical_spann_storage_audit.rs','rust/index/benches/sharding/storage_audit.rs','rust/index/tests/hierarchical_spann_storage_audit.rs'];(out/'source-manifest.json').write_text(json.dumps({f:hashlib.sha256((root/f).read_bytes()).hexdigest() for f in files},indent=2)+'\n')
with tarfile.open(out/'source.tar.gz','w:gz') as tar:
 for f in files:tar.add(root/f,arcname=f)
PY
cargo +1.92.0 bench -p chroma-index --bench hierarchical_spann_storage_audit --no-run --locked 2>&1 | tee /mnt/spann/results/storage-audit-v1/compile.log
python3 - <<'PY'
import pathlib,re,shutil,hashlib,json
out=pathlib.Path('/mnt/spann/results/storage-audit-v1');paths=re.findall(r'Executable.*\(([^)]+hierarchical_spann_storage_audit-[^)]+)\)',(out/'compile.log').read_text());assert paths
binary=pathlib.Path('/mnt/spann/hierarchical_spann_storage_audit-v1');assert not binary.exists();shutil.copy2(paths[-1],binary);record=dict(binary=str(binary),sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),source_archive_sha256=hashlib.sha256((out/'source.tar.gz').read_bytes()).hexdigest());(out/'binary.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(dict(run='storage-diagnostic-build',status='passed',**record)),flush=True)
PY
cargo +1.92.0 test -p chroma-index --test hierarchical_spann_storage_audit --release --locked 2>&1 | tee /mnt/spann/results/storage-audit-v1/tests.log
printf '%s\n' '{"run":"storage-diagnostic-tests","status":"passed"}'
