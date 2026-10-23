#!/usr/bin/env bash
set -euo pipefail
r=/home/ubuntu/benchmark-data/wikipedia-10m-scaling-baseline-20261008
audit=/home/ubuntu/chroma-wikipedia-10m-audit
mkdir "$audit"
tar -xf /home/ubuntu/benchmark-data/wikipedia-10m-code.tar -C "$audit"
python3 - "$audit" <<'PY'
import sys
from pathlib import Path
p=Path(sys.argv[1])/'rust/index/benches/hierarchical_spann_profile_quantized.rs'
s=p.read_text()
needle='    writer.print_tree_stats(format_count, Some(total_vectors));'
assert s.count(needle)==1
s=s.replace(needle, '''    if std::env::var_os("SCALING_EXACT_POSTING_AUDIT").is_some() {
        let valid = writer.root_reachable_valid_ids()?;
        if valid.len() != total_vectors || valid.iter().any(|id| *id as usize >= total_vectors) {
            return Err(format!("persisted posting audit failed: {} valid IDs for {} expected", valid.len(), total_vectors).into());
        }
        println!("AUDIT PASSED: {} persisted root-reachable valid IDs equal [0, {}); all child links resolve", valid.len(), total_vectors);
        return Ok(());
    }
''' + needle)
p.write_text(s)
PY
diff -u /home/ubuntu/chroma-wikipedia-10m/rust/index/benches/hierarchical_spann_profile_quantized.rs "$audit/rust/index/benches/hierarchical_spann_profile_quantized.rs" > "$r/audit-only.patch" || test "$?" = 1
cd "$audit"
export PATH=/home/ubuntu/.cargo/bin:$PATH
export CARGO_TARGET_DIR=/home/ubuntu/chroma-benchmark/target
cargo clean -p chroma-index --release > "$r/audit-clean.log" 2>&1
cargo bench -p chroma-index --bench hierarchical_spann_profile_quantized --no-run > "$r/audit-build.log" 2>&1
binary=$(find "$CARGO_TARGET_DIR/release/deps" -maxdepth 1 -name 'hierarchical_spann_profile_quantized-*' -type f -executable | head -1)
cp "$binary" "$r/audit-binary"
sha256sum "$r/audit-binary" > "$r/audit-binary.sha256"
export HOME="$r/home"
export HF_HOME=/mnt/data/huggingface
export SCALING_EXACT_POSTING_AUDIT=1
command=("$r/audit-binary" --dataset wikipedia-en --metric l2 --checkpoint 10 --checkpoint-size 1000000 --resume --no-commit --save-dir /mnt/data/wikipedia-10m-scaling-baseline-snapshots-20261008/10000000 --write-navigation fp --fp-npa --threads 32 --balance-threads 32 --write-beam-tau 0.5 --write-level-taus _,_,0.30 --write-rng-factor 0 --num-queries 0 --recall-tau-values 1.5 --recall-rerank-vectors 1 --max-cache-bytes 1073741824 --compute-gt-clusters false)
printf '%q ' "${command[@]}" > "$r/audit.command"
printf '\n' >> "$r/audit.command"
/usr/bin/time -v -o "$r/audit.time" "${command[@]}" > "$r/audit.log" 2>&1
printf '%s\n' complete > "$r/audit-status"
