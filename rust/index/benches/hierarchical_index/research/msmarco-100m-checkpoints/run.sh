#!/bin/bash
set -euo pipefail
out=/mnt/msmarco/results
exec 9>/mnt/data/chroma-benchmark.lock
flock 9
trap 'code=$?; printf "%s\n" "$code" > "$out/pipeline.status"; date -u +%FT%TZ > "$out/finished-at"' EXIT
date -u +%FT%TZ > "$out/started-at"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export HF_HOME=/mnt/msmarco/huggingface
export CARGO_TARGET_DIR=/mnt/msmarco/target
export CARGO_HOME=/mnt/msmarco/cargo-home
export TMPDIR=/mnt/msmarco/tmp
export CHROMA_BENCH_GT_PATH=/mnt/msmarco/results/msmarco_39m_100m_ground_truth.parquet
mkdir -p "$TMPDIR"
printf 'Preparing dataset and compiler artifacts.\n' > "$out/phase.txt"
/home/ubuntu/chroma/venv/bin/python "$out/download.py" > "$out/download.log" 2>&1 &
download_pid=$!
trap 'code=$?; kill "$download_pid" 2>/dev/null || true; printf "%s\n" "$code" > "$out/pipeline.status"; date -u +%FT%TZ > "$out/finished-at"' EXIT
cp -a /home/ubuntu/chroma-benchmark/target "$CARGO_TARGET_DIR"
cp -a /home/ubuntu/.cargo "$CARGO_HOME"
cd /mnt/msmarco/source
python3 - <<'PY'
from pathlib import Path
p=Path('rust/index/benches/datasets/msmarco.rs')
s=p.read_text()
old='fn gt_path() -> PathBuf {\n    cache_dir().join("ground_truth.parquet")\n}'
new='fn gt_path() -> PathBuf {\n    std::env::var_os("CHROMA_BENCH_GT_PATH")\n        .map(PathBuf::from)\n        .unwrap_or_else(|| cache_dir().join("ground_truth.parquet"))\n}'
assert s.count(old)==1
p.write_text(s.replace(old,new))
Path('/mnt/msmarco/results/ground-truth-path-overlay.txt').write_text(new+'\n')
p=Path('rust/index/benches/hierarchical_spann_profile_quantized.rs')
s=p.read_text()
old='    build_wall.enter(Phase::SummaryStats);\n    writer.print_tree_stats(format_count, Some(total_vectors));'
new='''    let (reachable_ids, missing_embeddings, missing_children) = writer.reachable_valid_posting_counts();
    println!("Persisted checkpoint audit: reachable valid IDs={} expected={} missing embeddings={} missing children={}", reachable_ids, total_vectors, missing_embeddings, missing_children);
    if reachable_ids != total_vectors || missing_embeddings != 0 || missing_children != 0 {
        return Err("persisted checkpoint audit failed".into());
    }
    build_wall.enter(Phase::SummaryStats);
    writer.print_tree_stats(format_count, Some(total_vectors));'''
assert s.count(old)==1
p.write_text(s.replace(old,new))
Path('/mnt/msmarco/results/persisted-audit-overlay.txt').write_text(new+'\n')
PY
cargo +stable bench --locked -p chroma-index --bench hierarchical_spann_profile_quantized --no-run -j 24 > "$out/build.log" 2>&1
python3 - <<'PY'
import pathlib,shutil
p=pathlib.Path('/mnt/msmarco/target/release/deps')
files=[f for f in p.glob('hierarchical_spann_profile_quantized-*') if f.is_file() and f.stat().st_mode & 0o111]
f=max(files,key=lambda x:x.stat().st_mtime_ns)
shutil.copy2(f,'/mnt/msmarco/results/binary')
PY
sha256sum "$out/binary" > "$out/binary.sha256"
"$out/binary" --help > "$out/help.txt"
wait "$download_pid"
printf 'Building 39M and 99M checkpoints.\n' > "$out/phase.txt"
/usr/bin/time -v -o "$out/build-checkpoints.time" python3 "$out/run-build.py" > "$out/build-controller.log" 2>&1
printf 'Computing exact answers at 39M, 40M, 99M and 100M.\n' > "$out/phase.txt"
export OPENBLAS_NUM_THREADS=32
/usr/bin/time -v -o "$out/ground-truth.time" /home/ubuntu/chroma/venv/bin/python "$out/exact-gt.py" msmarco --source-manifest "$out/msmarco_100m_sources.json" --outdir "$out" > "$out/ground-truth.log" 2>&1
printf 'Testing 39M to 40M and 99M to 100M from independent writable copies.\n' > "$out/phase.txt"
/usr/bin/time -v -o "$out/verify-increments.time" python3 "$out/verify-increments.py" > "$out/verify-increments.log" 2>&1
printf 'Complete: 39M and 99M preserved; final 1M increments and recall verified.\n' > "$out/phase.txt"
