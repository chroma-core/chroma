#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
out_dir="${TMPDIR:-/tmp}/gha-runtime-report-test-$$"

mkdir -p "$out_dir"

"$repo_root/bin/ci/gha-runtime-report.sh" \
  --jobs-file "$repo_root/bin/ci/fixtures/gha-runtime-jobs.json" \
  --repo chroma-core/chroma \
  --run-id 123 \
  --attempt 1 \
  --out-dir "$out_dir" >/dev/null

jq -e '
  .total_jobs == 3
  and .completed_jobs == 2
  and (
    .jobs[]
    | select(.name == "Python tests / test-rust-bindings (3.9, chromadb/test)")
    | .duration_s == 600
  )
  and (
    .jobs[]
    | select(.name == "Rust tests / Integration test ci_k8s_integration 1")
    | .duration_s == 2730
  )
  and (
    .steps[]
    | select(.job_name == "Rust tests / Integration test ci_k8s_integration 1" and .name == "Run tests")
    | .duration_s == 1620
  )
' "$out_dir/gha-runtime-report.json" >/dev/null

grep -q "Completed Jobs by Duration" "$out_dir/gha-runtime-report.md"
grep -q "Rust tests / Integration test ci_k8s_integration 1" "$out_dir/gha-runtime-report.csv"

echo "gha-runtime-report fixture test passed: $out_dir"
