#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$PATH"
results=/home/ubuntu/benchmark-data/destination-deltas-7842
repo=/home/ubuntu/chroma-reassignment-destination-deltas
python3 "$results/audit-source.py"
trap 'cp "$results/audit-source-original.rs" "$repo/rust/index/benches/hierarchical_spann_profile_quantized.rs"' EXIT
cd "$repo"
CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target cargo build --release -p chroma-index --bench hierarchical_spann_profile_quantized --message-format=json > "$results/audit-build.jsonl" 2> "$results/audit-build.log"
binary=$(python3 - "$results/audit-build.jsonl" <<'PY'
import json,sys
for line in open(sys.argv[1]):
 try: a=json.loads(line)
 except json.JSONDecodeError: continue
 if a.get('reason')=='compiler-artifact' and a.get('target',{}).get('name')=='hierarchical_spann_profile_quantized' and a.get('executable'): print(a['executable'])
PY
)
AUDIT_SOURCE_ONLY=1 "$binary" --dataset ms-marco --metric l2 --checkpoint 8 --checkpoint-size 100000 --resume --save-dir /mnt/data/balance-diagnostics-source --no-commit --write-navigation fp --fp-npa --threads 64 --balance-threads 64 --write-beam-tau 1.5 --write-rng-factor 4 --max-cache-bytes 1073741824 > "$results/audit-source.log" 2>&1
