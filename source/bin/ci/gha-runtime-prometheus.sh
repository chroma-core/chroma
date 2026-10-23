#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: gha-runtime-prometheus.sh [options]

Fetch gha-runtime-report.json artifacts from recent GitHub Actions runs and
emit Prometheus text metrics for job runtimes.

Options:
  --repo REPO              GitHub repository, e.g. chroma-core/chroma.
                           Defaults to GITHUB_REPOSITORY, then gh repo view.
  --since TIMESTAMP        Fetch workflow runs created at or after TIMESTAMP.
                           Defaults to seven days ago.
  --days DAYS              Look back DAYS days when --since is not provided.
                           Defaults to 7.
  --artifact-name NAME     Artifact to fetch. Defaults to gha-runtime-report.
  --metric-name NAME       Metric name. Defaults to
                           github_actions_job_runtime_seconds.
  --work-dir DIR           Directory for temporary downloads. Defaults to mktemp.
  --keep-work-dir          Keep temporary downloads after completion.
  -h, --help               Show this help.

The output is Prometheus text format on stdout. Progress and skipped artifacts
are reported on stderr.
USAGE
}

repo="${GITHUB_REPOSITORY:-}"
since=""
days=7
artifact_name="gha-runtime-report"
metric_name="github_actions_job_runtime_seconds"
work_dir=""
keep_work_dir=0

while (($#)); do
  case "$1" in
    --repo)
      repo="${2:?missing value for --repo}"
      shift 2
      ;;
    --since)
      since="${2:?missing value for --since}"
      shift 2
      ;;
    --days)
      days="${2:?missing value for --days}"
      shift 2
      ;;
    --artifact-name)
      artifact_name="${2:?missing value for --artifact-name}"
      shift 2
      ;;
    --metric-name)
      metric_name="${2:?missing value for --metric-name}"
      shift 2
      ;;
    --work-dir)
      work_dir="${2:?missing value for --work-dir}"
      shift 2
      ;;
    --keep-work-dir)
      keep_work_dir=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$days" =~ ^[0-9]+$ ]] || [[ "$days" -eq 0 ]]; then
  echo "--days must be a positive integer" >&2
  exit 2
fi

if ! [[ "$metric_name" =~ ^[a-zA-Z_:][a-zA-Z0-9_:]*$ ]]; then
  echo "--metric-name must be a valid Prometheus metric name" >&2
  exit 2
fi

for dependency in gh jq unzip; do
  if ! command -v "$dependency" >/dev/null 2>&1; then
    echo "$dependency is required" >&2
    exit 1
  fi
done

default_since() {
  local lookback_days="$1"

  if date -u -v-"${lookback_days}"d +"%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1; then
    date -u -v-"${lookback_days}"d +"%Y-%m-%dT%H:%M:%SZ"
    return
  fi

  if date -u -d "${lookback_days} days ago" +"%Y-%m-%dT%H:%M:%SZ" >/dev/null 2>&1; then
    date -u -d "${lookback_days} days ago" +"%Y-%m-%dT%H:%M:%SZ"
    return
  fi

  python3 - "$lookback_days" <<'PY'
from datetime import datetime, timedelta, timezone
import sys

days = int(sys.argv[1])
print((datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
}

if [[ -z "$repo" ]]; then
  repo="$(gh repo view --json nameWithOwner --jq '.nameWithOwner')"
fi

if [[ -z "$repo" ]]; then
  echo "--repo is required when the current directory is not a gh repository" >&2
  exit 2
fi

if [[ -z "$since" ]]; then
  since="$(default_since "$days")"
fi

if [[ -z "$work_dir" ]]; then
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/gha-runtime-prometheus.XXXXXX")"
else
  mkdir -p "$work_dir"
fi

if [[ "$keep_work_dir" -eq 0 ]]; then
  trap 'rm -rf "$work_dir"' EXIT
else
  echo "keeping work dir: $work_dir" >&2
fi

runs_json="$work_dir/runs.json"
artifacts_jsonl="$work_dir/artifacts.jsonl"
: > "$artifacts_jsonl"

echo "fetching workflow runs for $repo since $since" >&2
gh api --method GET --paginate "repos/${repo}/actions/runs" \
  -f per_page=100 \
  -f "created=>=${since}" \
  --jq '.workflow_runs[]?' \
  | jq -s '.' > "$runs_json"

run_count="$(jq 'length' "$runs_json")"
echo "found $run_count workflow runs" >&2

jq -r '.[].id' "$runs_json" | while read -r run_id; do
  gh api --method GET --paginate "repos/${repo}/actions/runs/${run_id}/artifacts" \
    -f per_page=100 \
    --jq '.artifacts[]?' \
    | jq -c --arg name "$artifact_name" --argjson run_id "$run_id" '
        select(.name == $name and (.expired | not))
        | . + {run_id: $run_id}
      ' >> "$artifacts_jsonl"
done

artifact_count="$(wc -l < "$artifacts_jsonl" | tr -d ' ')"
echo "found $artifact_count $artifact_name artifacts" >&2

cat <<EOF
# HELP ${metric_name} GitHub Actions job runtime from gha-runtime-report.json artifacts.
# TYPE ${metric_name} gauge
EOF

while IFS= read -r artifact; do
  artifact_id="$(jq -r '.id' <<<"$artifact")"
  run_id="$(jq -r '.run_id' <<<"$artifact")"
  artifact_dir="$work_dir/artifacts/$artifact_id"
  artifact_zip="$artifact_dir.zip"

  mkdir -p "$artifact_dir"

  echo "downloading artifact $artifact_id for run $run_id" >&2
  gh api "repos/${repo}/actions/artifacts/${artifact_id}/zip" > "$artifact_zip"
  unzip -qq "$artifact_zip" -d "$artifact_dir"

  report_json="$(find "$artifact_dir" -type f -name 'gha-runtime-report.json' -print -quit)"
  if [[ -z "$report_json" ]]; then
    echo "skipping artifact $artifact_id: no gha-runtime-report.json found" >&2
    continue
  fi

  run_json="$(
    jq -c --argjson run_id "$run_id" '
      map(select(.id == $run_id))[0] // {}
    ' "$runs_json"
  )"

  jq -r \
    --arg metric_name "$metric_name" \
    --arg repository "$repo" \
    --arg artifact_id "$artifact_id" \
    --argjson run "$run_json" \
    '
      def prom_escape:
        tostring
        | gsub("\\\\"; "\\\\\\\\")
        | gsub("\n"; "\\n")
        | gsub("\""; "\\\"");

      def label_pairs($labels):
        $labels
        | to_entries
        | map("\(.key)=\"\(.value | prom_escape)\"")
        | join(",");

      def runner:
        if (.runner_name // "") != "" then
          .runner_name
        else
          (.labels // [] | join(","))
        end;

      . as $report
      | ($report.jobs // [])[]
      | select(.duration_s != null)
      | {
          repository: $repository,
          workflow: ($run.name // ""),
          event: ($run.event // ""),
          branch: ($run.head_branch // ""),
          sha: ($run.head_sha // ""),
          run_id: (($report.run_id // $run.id // "") | tostring),
          run_attempt: (($report.run_attempt // "") | tostring),
          run_number: (($run.run_number // "") | tostring),
          artifact_id: $artifact_id,
          job: (.name // ""),
          job_id: ((.id // "") | tostring),
          conclusion: (.conclusion // ""),
          runner: runner,
          started_at: (.started_at // ""),
          completed_at: (.completed_at // "")
        } as $labels
      | "\($metric_name){\(label_pairs($labels))} \(.duration_s)"
    ' "$report_json"
done < "$artifacts_jsonl"
