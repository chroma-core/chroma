#!/usr/bin/env bash
set -euo pipefail

out=/home/ubuntu/benchmark-data/reassignment-experiments-20261012
root=/mnt/data/reassignment-experiments-20261012
checkpoint=${TARGET_CHECKPOINT:-10}
case "$checkpoint" in 10|20|30|40) ;; *) echo 'Use checkpoint 10, 20, 30, or 40.' >&2; exit 2;; esac
source_vectors=$(( (checkpoint - 1) * 1000000 ))
expected_vectors=$(( checkpoint * 1000000 ))
fixture=/mnt/data/wikipedia-40m-baseline-20261009-snapshots/$source_vectors
results="$out/cp-$checkpoint"
mkdir -p "$root" "$results"
exec 9>/mnt/data/chroma-benchmark.lock
flock -n 9 || { echo 'Another benchmark owns the machine; retry after it finishes.' >&2; exit 75; }
trap 'printf "%s\n" "$?" > "$results/run.status"' EXIT
export HF_HOME=/mnt/data/huggingface HF_HUB_OFFLINE=1
test -f "$fixture/checkpoint.json"
test -f /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet
for variant in raw baseline deferred batched; do test -x "$out/$variant-binary"; done
test -x "$out/audit-binary"
sha256sum /home/ubuntu/.cache/wikipedia_en/ground_truth.parquet > "$results/ground-truth.sha256"

# This reads the immutable input without committing or changing its metadata.
"$out/audit-binary" "$fixture" "$source_vectors" > "$results/source.audit.json"
python3 - "$fixture" "$results/source.manifest.json" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1])
manifest={}
for p in sorted(root.rglob('*')):
    if p.is_symlink():
        raise RuntimeError(f'Snapshot contains a symlink: {p}')
    if p.is_file():
        with p.open('rb') as stream:
            digest=hashlib.sha256()
            for block in iter(lambda:stream.read(8*1024*1024),b''):
                digest.update(block)
            manifest[str(p.relative_to(root))]=digest.hexdigest()
Path(sys.argv[2]).write_text(json.dumps(manifest,sort_keys=True))
PY

settings=(--dataset wikipedia-en --metric l2 --checkpoint "$checkpoint" --checkpoint-size 1000000
    --branching-factor 100 --split-threshold 2048 --merge-threshold 512
    --threads 32 --balance-threads 32 --write-navigation fp --fp-npa
    --write-beam-tau 0.5 --write-beam-min 10 --write-beam-max 16 --write-level-taus _,_,0.30
    --max-replicas 1 --write-rng-epsilon 0 --write-rng-factor 0
    --read-beam-min 10 --read-beam-max 256 --read-level-taus _,_,_ --read-level-min-pcts 50,5,0
    --recall-rerank-vectors 1,8 --recall-tau-values 0.7,1,2 --num-queries 100
    --max-cache-bytes 1073741824 --compute-gt-clusters false --verify-valid-postings)

# Raw controls measure instrumentation overhead. Other variants share identical counters.
order=(raw-1 baseline-1 deferred-1 batched-1 batched-2 deferred-2 baseline-2 raw-2
       deferred-3 raw-3 batched-3 baseline-3)
for name in "${order[@]}"; do
    variant=${name%-*}
    work="$root/cp-$checkpoint-$name"
    test ! -e "$work"
    test ! -e "$results/$name.log"
    cp -a --reflink=auto "$fixture" "$work"
    python3 - "$fixture" "$work" <<'PY'
import os,sys
from pathlib import Path
source,dest=map(Path,sys.argv[1:])
for p in dest.rglob('*'):
    if p.is_file():
        assert os.stat(p).st_ino != os.stat(source/p.relative_to(dest)).st_ino, p
PY
    command=("$out/$variant-binary" "${settings[@]}" --save --resume --save-dir "$work")
    printf '%q ' "${command[@]}" > "$results/$name.command"
    printf '\n' >> "$results/$name.command"
    /usr/bin/time -v -o "$results/$name.time" "${command[@]}" > "$results/$name.log" 2>&1
    python3 - "$results/$name.log" "$expected_vectors" <<'PY'
import sys
from pathlib import Path
text=Path(sys.argv[1]).read_text()
expected=int(sys.argv[2])
assert f'Pre-commit reachable valid IDs: {expected} | indexed embeddings without one: 0 | missing child nodes: 0' in text
assert 'Recall (100 queries, k=100' in text, 'Ground truth does not contain 100 queries for this prefix'
assert 'using queries keyed at full corpus' not in text, 'Ground truth uses another corpus size'
PY
    # Persistence validation is separate from the measured run.
    /usr/bin/time -v -o "$results/$name.audit.time" "$out/audit-binary" "$work" "$expected_vectors" \
        > "$results/$name.audit.json" 2> "$results/$name.audit.log"
    cp "$work/checkpoint.json" "$results/$name.checkpoint.json"
    sha256sum -c "$results/ground-truth.sha256"
    printf '%s completed at %s\n' "$name" "$(date -u +%FT%TZ)" >> "$results/progress.log"
    # Retain selected validated copies for follow-on checkpoints, when requested.
    case ",${RETAIN_REPLAYS:-}," in
        *",$name,"*) ;;
        *)
    # Remove only this job's validated replay copy, keeping all evidence and source snapshots.
            python3 - "$work" "$root" <<'PY'
import shutil,sys
from pathlib import Path
work,root=map(lambda s:Path(s).resolve(),sys.argv[1:])
assert work.parent==root and work.name.startswith('cp-')
shutil.rmtree(work)
PY
            ;;
    esac
done

python3 - "$fixture" "$results/source.manifest.json" <<'PY'
import hashlib,json,sys
from pathlib import Path
root=Path(sys.argv[1])
expected=json.loads(Path(sys.argv[2]).read_text())
actual={}
for p in sorted(root.rglob('*')):
    if p.is_file():
        with p.open('rb') as stream:
            digest=hashlib.sha256()
            for block in iter(lambda:stream.read(8*1024*1024),b''):
                digest.update(block)
            actual[str(p.relative_to(root))]=digest.hexdigest()
assert actual==expected, 'The source snapshot changed'
PY
echo 'All replays passed persistence audits; the source snapshot is unchanged.'
