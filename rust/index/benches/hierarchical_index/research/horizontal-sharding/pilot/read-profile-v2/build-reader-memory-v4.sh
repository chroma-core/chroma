#!/usr/bin/env bash
set -euo pipefail
cd /mnt/spann/source
export CARGO_HOME=/mnt/spann/cargo
export RUSTUP_HOME=/mnt/spann/rustup
export CARGO_TARGET_DIR=/mnt/spann/target-reader-memory-v4
export TMPDIR=/mnt/spann/tmp
export CARGO_BUILD_JOBS=16
export TOKIO_WORKER_THREADS=2
mkdir -p "$CARGO_HOME/bin" "$RUSTUP_HOME" "$CARGO_TARGET_DIR" "$TMPDIR" /mnt/spann/results/reader-memory-v4
cp -a /home/ubuntu/.cargo/bin/. "$CARGO_HOME/bin/"
export PATH="$CARGO_HOME/bin:$PATH"
rustup toolchain install 1.92.0 --profile minimal 2>&1 | tee /mnt/spann/results/reader-memory-v4/toolchain.log
rustc +1.92.0 -Vv > /mnt/spann/results/reader-memory-v4/compiler.txt
/usr/bin/time -v cargo +1.92.0 bench -p chroma-index --bench hierarchical_spann_shard --no-run --locked 2>&1 | tee /mnt/spann/results/reader-memory-v4/compile.log
python3 - <<'PY'
import pathlib,re,shutil,hashlib,json
r=pathlib.Path('/mnt/spann/results/reader-memory-v4');paths=re.findall(r'Executable.*\(([^)]+hierarchical_spann_shard-[^)]+)\)',(r/'compile.log').read_text());assert paths
binary=pathlib.Path('/mnt/spann/hierarchical_spann_shard-memory-v4');assert not binary.exists();shutil.copy2(paths[-1],binary)
identity=dict(binary=str(binary),sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),source_archive_sha256=hashlib.sha256((r/'source.tar.gz').read_bytes()).hexdigest());(r/'binary.json').write_text(json.dumps(identity,indent=2)+'\n');print(json.dumps(dict(run='optimized-bench-build',status='passed',**identity)),flush=True)
PY
cargo +1.92.0 test -p chroma-index --test hierarchical_spann_reader_integrity --release --locked 2>&1 | tee /mnt/spann/results/reader-memory-v4/tests.log
printf '%s\n' '{"run":"release-integrity-tests","status":"passed"}'
python3 rust/index/benches/sharding/smoke.py /mnt/spann/hierarchical_spann_shard-memory-v4 2>&1 | tee /mnt/spann/results/reader-memory-v4/smoke-baseline.log
printf '%s\n' '{"run":"baseline-smoke","status":"passed"}'
python3 rust/index/benches/sharding/remote_tree_smoke.py /mnt/spann/hierarchical_spann_shard-memory-v4 2>&1 | tee /mnt/spann/results/reader-memory-v4/smoke-tree.log
printf '%s\n' '{"run":"tree-and-memory-metrics-smoke","status":"passed"}'
python3 rust/index/benches/sharding/audit_smoke.py /mnt/spann/hierarchical_spann_shard-memory-v4 2>&1 | tee /mnt/spann/results/reader-memory-v4/smoke-audit.log
printf '%s\n' '{"run":"full-audit-smoke","status":"passed"}'
