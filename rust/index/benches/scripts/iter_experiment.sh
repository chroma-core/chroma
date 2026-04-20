#!/usr/bin/env bash
# Set up (and optionally run) a profiling iteration on top of an existing
# hierarchical_spann_profile_quantized save dir without mutating the source.
#
# The bench's resume path is keyed on (checkpoint_idx, checkpoint_size):
# loop iter `i` reads `dataset[i*checkpoint_size .. (i+1)*checkpoint_size]`.
# To add a small batch on top of an existing save with a smaller per-iter
# size, we rewrite checkpoint.json so the next iter's offset still lands
# at the same total_vectors. Concretely, with SOURCE total_vectors = T and
# new --checkpoint-size = S, set checkpoint_idx = (T / S) - 1.
#
# Strategy:
#   - hardlink the (large) `data/` blockfiles dir from SOURCE into ITER
#     (cheap; near-zero disk cost; preserves SOURCE inodes)
#   - write a fresh checkpoint.json into ITER with the recomputed
#     checkpoint_idx (NOT a hardlink; bench would truncate it)
#   - reuse SOURCE's posting_list_id / scalar_metadata_id /
#     vector_data_id / list_data_id verbatim
#
# The bench's `save_checkpoint_meta` does an atomic write+rename, so even
# if checkpoint.json were hardlinked the SOURCE would be safe; this script
# still copies it as a non-hardlink defensively.
#
# Usage:
#
#   # Just set up an iter dir and print the suggested cargo bench command:
#   ./iter_experiment.sh setup
#   ./iter_experiment.sh setup /mnt/data2/msmarco_en_114m_save_55m_experiment
#
#   # Set up AND run the bench (one CP of CHECKPOINT_SIZE vectors,
#   # add+balance only with --no-commit):
#   ./iter_experiment.sh run
#
# Tunables (env vars, all optional):
#
#   SOURCE             Path to the saved index to resume from.
#                      Default: /mnt/data2/msmarco_en_114m_save_55m_experiment
#   ITER_PARENT        Where to place the iteration dir.
#                      Default: dirname of SOURCE (so it lives next to it)
#   ITER_SUFFIX        Suffix for the iteration dir name.
#                      Default: iter_<UTC timestamp>
#   CHECKPOINT_SIZE    Vectors to add in the one iteration. Default: 100000
#   NO_COMMIT          1 (default) to pass --no-commit, 0 to commit normally.
#   MAX_CACHE_BYTES    Default: 8 GiB
#   THREADS            Default: 32
#   DATASET            Default: ms-marco
#   EXTRA_BENCH_ARGS   Additional args appended to cargo bench (e.g. tree
#                      tuning flags from your 114M run). No default.

set -euo pipefail

SOURCE="${SOURCE:-${2:-/mnt/data2/msmarco_en_114m_save_55m_experiment}}"
ITER_PARENT="${ITER_PARENT:-$(dirname "$SOURCE")}"
ITER_SUFFIX="${ITER_SUFFIX:-iter_$(date -u +%Y%m%d_%H%M%S)}"
ITER="${ITER:-$ITER_PARENT/$(basename "$SOURCE")_${ITER_SUFFIX}}"

CHECKPOINT_SIZE="${CHECKPOINT_SIZE:-100000}"
NO_COMMIT="${NO_COMMIT:-1}"
MAX_CACHE_BYTES="${MAX_CACHE_BYTES:-$((8 * 1024 * 1024 * 1024))}"
THREADS="${THREADS:-32}"
DATASET="${DATASET:-ms-marco}"
EXTRA_BENCH_ARGS="${EXTRA_BENCH_ARGS:-}"

cmd="${1:-setup}"

err() { echo "iter_experiment: $*" >&2; exit 1; }

require_python() {
  command -v python3 >/dev/null || err "python3 required (json parsing)"
}

read_source_total_vectors() {
  require_python
  python3 - "$SOURCE/checkpoint.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    meta = json.load(f)
print(meta["total_vectors"])
PY
}

write_iter_checkpoint() {
  local total="$1" new_idx="$2"
  require_python
  python3 - "$SOURCE/checkpoint.json" "$ITER/checkpoint.json" "$new_idx" "$total" <<'PY'
import json, sys
src, dst, new_idx, total = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
with open(src) as f:
    meta = json.load(f)
meta["checkpoint_idx"] = new_idx
meta["total_vectors"] = total
with open(dst, "w") as f:
    json.dump(meta, f, indent=2)
PY
}

setup() {
  [[ -d "$SOURCE" ]] || err "SOURCE does not exist: $SOURCE"
  [[ -f "$SOURCE/checkpoint.json" ]] || err "missing checkpoint.json in $SOURCE"
  [[ -d "$SOURCE/data" ]] || err "missing data/ in $SOURCE"

  if [[ -e "$ITER" ]]; then
    err "ITER already exists: $ITER (set ITER or ITER_SUFFIX)"
  fi

  local total
  total="$(read_source_total_vectors)"
  if (( total % CHECKPOINT_SIZE != 0 )); then
    err "SOURCE total_vectors ($total) is not a multiple of CHECKPOINT_SIZE ($CHECKPOINT_SIZE); pick a different size or fix the source"
  fi
  local new_idx=$(( total / CHECKPOINT_SIZE - 1 ))
  local stop_at=$(( new_idx + 2 ))   # run exactly one CP

  mkdir -p "$ITER"
  # ITER/data does not exist yet (we errored out above if ITER did);
  # `cp -al SRC DST` materializes DST as a hardlinked mirror of SRC.
  cp -al "$SOURCE/data" "$ITER/data"
  write_iter_checkpoint "$total" "$new_idx"

  echo "=== iter setup ==="
  echo "  SOURCE:           $SOURCE  (total_vectors=$total)"
  echo "  ITER:             $ITER"
  echo "  CHECKPOINT_SIZE:  $CHECKPOINT_SIZE"
  echo "  rewritten checkpoint_idx: $new_idx (so next iter offset = $((new_idx + 1)) * $CHECKPOINT_SIZE = $total)"
  echo "  --checkpoint:     $stop_at  (loop runs exactly 1 iter)"
  echo "  NO_COMMIT:        $NO_COMMIT"
  echo "  ITER size:        $(du -sh "$ITER" 2>/dev/null | awk '{print $1}')  (should be tiny -- hardlinks)"
  echo
  echo "  Suggested run:"
  print_run_cmd "$stop_at"
}

print_run_cmd() {
  local stop_at="$1"
  local nc=""
  (( NO_COMMIT == 1 )) && nc=" --no-commit"
  cat <<EOF

    cargo bench --bench hierarchical_spann_profile_quantized -p chroma-index --release -- \\
      --dataset $DATASET \\
      --save-dir "$ITER" \\
      --resume \\
      --checkpoint-size $CHECKPOINT_SIZE \\
      --checkpoint $stop_at \\
      --max-cache-bytes $MAX_CACHE_BYTES \\
      --threads $THREADS \\
      --gc-blockfiles=false${nc} \\
      $EXTRA_BENCH_ARGS

EOF
}

run() {
  setup
  local total new_idx stop_at
  total="$(read_source_total_vectors)"
  new_idx=$(( total / CHECKPOINT_SIZE - 1 ))
  stop_at=$(( new_idx + 2 ))
  local nc=()
  (( NO_COMMIT == 1 )) && nc=(--no-commit)

  : "${HF_HOME:=/mnt/data/huggingface}"
  : "${_RJEM_MALLOC_CONF:=background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:1000}"
  export HF_HOME _RJEM_MALLOC_CONF

  echo "=== running iter bench ==="
  echo "  HF_HOME=$HF_HOME"
  echo "  _RJEM_MALLOC_CONF=$_RJEM_MALLOC_CONF"
  echo

  # shellcheck disable=SC2086  # EXTRA_BENCH_ARGS is intentionally word-split
  cargo bench --bench hierarchical_spann_profile_quantized -p chroma-index --release -- \
    --dataset "$DATASET" \
    --save-dir "$ITER" \
    --resume \
    --checkpoint-size "$CHECKPOINT_SIZE" \
    --checkpoint "$stop_at" \
    --max-cache-bytes "$MAX_CACHE_BYTES" \
    --threads "$THREADS" \
    --gc-blockfiles=false \
    "${nc[@]}" \
    $EXTRA_BENCH_ARGS
}

case "$cmd" in
  setup) setup ;;
  run)   run ;;
  *)     err "unknown subcommand: $cmd (expected 'setup' or 'run')" ;;
esac
