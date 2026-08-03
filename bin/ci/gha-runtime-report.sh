#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: gha-runtime-report.sh [options]

Generate a GitHub Actions runtime report for the current workflow run.

Options:
  --repo REPO              GitHub repository, e.g. chroma-core/chroma.
                           Defaults to GITHUB_REPOSITORY.
  --run-id RUN_ID          GitHub Actions run id. Defaults to GITHUB_RUN_ID.
  --attempt ATTEMPT        GitHub Actions run attempt. Defaults to
                           GITHUB_RUN_ATTEMPT, then 1.
  --out-dir DIR            Directory for report files. Defaults to
                           gha-runtime-report.
  --jobs-file FILE         Read jobs JSON from FILE instead of gh api.
                           FILE may be an API response object with a jobs
                           array or a raw array of jobs.
  --max-step-rows COUNT    Maximum rows for step tables. Defaults to 50.
  -h, --help               Show this help.

Outputs:
  gha-runtime-report.md
  gha-runtime-report.json
  gha-runtime-report.csv
USAGE
}

repo="${GITHUB_REPOSITORY:-}"
run_id="${GITHUB_RUN_ID:-}"
attempt="${GITHUB_RUN_ATTEMPT:-1}"
out_dir="gha-runtime-report"
jobs_file=""
max_step_rows="${GHA_RUNTIME_REPORT_STEP_ROWS:-50}"

while (($#)); do
  case "$1" in
    --repo)
      repo="${2:?missing value for --repo}"
      shift 2
      ;;
    --run-id)
      run_id="${2:?missing value for --run-id}"
      shift 2
      ;;
    --attempt)
      attempt="${2:?missing value for --attempt}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:?missing value for --out-dir}"
      shift 2
      ;;
    --jobs-file)
      jobs_file="${2:?missing value for --jobs-file}"
      shift 2
      ;;
    --max-step-rows)
      max_step_rows="${2:?missing value for --max-step-rows}"
      shift 2
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

if ! [[ "$max_step_rows" =~ ^[0-9]+$ ]]; then
  echo "--max-step-rows must be a non-negative integer" >&2
  exit 2
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to generate the runtime report" >&2
  exit 1
fi

mkdir -p "$out_dir"

raw_jobs="$out_dir/jobs.raw.json"
report_json="$out_dir/gha-runtime-report.json"
report_csv="$out_dir/gha-runtime-report.csv"
report_md="$out_dir/gha-runtime-report.md"
generated_at="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

if [[ -n "$jobs_file" ]]; then
  jq '
    if type == "object" and has("jobs") then
      .jobs
    elif type == "array" then
      .
    else
      error("jobs JSON must be an object with jobs or an array")
    end
  ' "$jobs_file" > "$raw_jobs"
else
  if [[ -z "$repo" || -z "$run_id" || -z "$attempt" ]]; then
    echo "--repo, --run-id, and --attempt are required outside GitHub Actions" >&2
    exit 2
  fi

  if ! command -v gh >/dev/null 2>&1; then
    echo "gh is required when --jobs-file is not provided" >&2
    exit 1
  fi

  gh api --paginate \
    "repos/${repo}/actions/runs/${run_id}/attempts/${attempt}/jobs?per_page=100" \
    --jq '.jobs[]?' |
    jq -s '.' > "$raw_jobs"
fi

jq \
  --arg generated_at "$generated_at" \
  --arg repository "$repo" \
  --arg run_id "$run_id" \
  --arg run_attempt "$attempt" \
  '
  def timestamp:
    if . == null or . == "" then
      null
    else
      (sub("\\.[0-9]+Z$"; "Z") | fromdateiso8601)
    end;

  def duration($start; $end):
    if $start == null or $start == "" or $end == null or $end == "" then
      null
    else
      (($end | timestamp) - ($start | timestamp) | floor)
    end;

  def human_duration:
    if . == null then
      ""
    else
      . as $total
      | (($total / 3600) | floor) as $hours
      | ((($total % 3600) / 60) | floor) as $minutes
      | (($total % 60) | floor) as $seconds
      | if $hours > 0 then
          "\($hours)h \($minutes)m \($seconds)s"
        elif $minutes > 0 then
          "\($minutes)m \($seconds)s"
        else
          "\($seconds)s"
        end
    end;

  def add_duration:
    . as $row
    | (duration($row.started_at; $row.completed_at)) as $duration
    | $row + {
        duration_s: $duration,
        duration_h: ($duration | human_duration)
      };

  def outcome:
    .conclusion // .status // "unknown";

  def normalized_step($job):
    {
      job_id: ($job.id // null),
      job_name: ($job.name // ""),
      job_conclusion: ($job | outcome),
      runner_name: (
        if ($job.runner_name // "") != "" then
          $job.runner_name
        else
          ($job.labels // [] | join(","))
        end
      ),
      number: (.number // null),
      name: (.name // ""),
      status: (.status // ""),
      conclusion: (.conclusion // .status // "unknown"),
      started_at: (.started_at // null),
      completed_at: (.completed_at // null),
      url: ($job.html_url // $job.url // "")
    }
    | add_duration;

  def normalized_job:
    . as $job
    | ({
        id: ($job.id // null),
        name: ($job.name // ""),
        status: ($job.status // ""),
        conclusion: ($job.conclusion // $job.status // "unknown"),
        runner_name: ($job.runner_name // ""),
        labels: ($job.labels // []),
        url: ($job.html_url // $job.url // ""),
        started_at: ($job.started_at // null),
        completed_at: ($job.completed_at // null)
      }
      | add_duration)
      + {
        steps: (($job.steps // []) | map(normalized_step($job)))
      };

  [.[] | normalized_job | select(.status != "in_progress")] as $jobs
  | {
      generated_at: $generated_at,
      repository: $repository,
      run_id: $run_id,
      run_attempt: $run_attempt,
      total_jobs: ($jobs | length),
      completed_jobs: ($jobs | map(select(.duration_s != null)) | length),
      jobs: $jobs,
      steps: (($jobs | map(.steps) | add) // [])
    }
  ' "$raw_jobs" > "$report_json"

jq -r '
  def outcome:
    .conclusion // .status // "unknown";

  def runner:
    if (.runner_name // "") != "" then
      .runner_name
    else
      (.labels // [] | join(","))
    end;

  [
    "type",
    "job",
    "step",
    "outcome",
    "runner",
    "started_at",
    "completed_at",
    "duration_s",
    "duration_h",
    "url"
  ],
  (.jobs[] | [
    "job",
    .name,
    "",
    outcome,
    runner,
    (.started_at // ""),
    (.completed_at // ""),
    (.duration_s // ""),
    (.duration_h // ""),
    (.url // "")
  ]),
  (.steps[] | [
    "step",
    .job_name,
    .name,
    outcome,
    (.runner_name // ""),
    (.started_at // ""),
    (.completed_at // ""),
    (.duration_s // ""),
    (.duration_h // ""),
    (.url // "")
  ])
  | @csv
' "$report_json" > "$report_csv"

jq -r --argjson max_step_rows "$max_step_rows" '
  def md:
    if . == null then
      ""
    else
      tostring
      | gsub("[|]"; "\\|")
      | gsub("\r"; " ")
      | gsub("\n"; " ")
    end;

  def outcome:
    .conclusion // .status // "unknown";

  def runner:
    if (.runner_name // "") != "" then
      .runner_name
    else
      (.labels // [] | join(","))
    end;

  def job_table($rows; $empty):
    if ($rows | length) == 0 then
      [$empty]
    else
      [
        "| Job | Outcome | Runner | Duration | Seconds | Started | Completed |",
        "| --- | --- | --- | ---: | ---: | --- | --- |"
      ]
      + ($rows | map(
        "| \(.name | md) | \(outcome | md) | \(runner | md) | \(.duration_h | md) | \(.duration_s // "") | \(.started_at | md) | \(.completed_at | md) |"
      ))
    end;

  def step_table($rows; $empty):
    if ($rows | length) == 0 then
      [$empty]
    else
      [
        "| Step | Job | Outcome | Runner | Duration | Seconds |",
        "| --- | --- | --- | --- | ---: | ---: |"
      ]
      + ($rows | map(
        "| \(.name | md) | \(.job_name | md) | \(outcome | md) | \(runner | md) | \(.duration_h | md) | \(.duration_s // "") |"
      ))
    end;

  def problem_table($rows):
    if ($rows | length) == 0 then
      ["_No completed jobs failed, cancelled, or skipped._"]
    else
      [
        "| Job | Outcome | Status | Runner | Duration |",
        "| --- | --- | --- | --- | ---: |"
      ]
      + ($rows | map(
        "| \(.name | md) | \(outcome | md) | \(.status | md) | \(runner | md) | \(.duration_h | md) |"
      ))
    end;

  def test_like:
    (.name | ascii_downcase | test("test|pytest|nextest|bench|integration|clippy|pre-commit|cargo fmt"));

  (.jobs | map(select(.duration_s != null)) | sort_by(.duration_s) | reverse) as $job_rows
  | (.steps | map(select(.duration_s != null)) | sort_by(.duration_s) | reverse | .[0:$max_step_rows]) as $step_rows
  | (.steps | map(select(.duration_s != null and test_like)) | sort_by(.duration_s) | reverse | .[0:$max_step_rows]) as $test_step_rows
  | (.jobs | map(select((.status // "") != "in_progress" and (outcome != "success"))) | sort_by(.name)) as $problem_rows
  | (
      .jobs
      | sort_by(outcome)
      | group_by(outcome)
      | map("**\((.[0] | outcome) | md)** \(length)")
      | join(", ")
    ) as $outcome_counts
  | [
      "# GitHub Actions Runtime Report",
      "",
      "Generated at: `\(.generated_at)`",
      "",
      "Repository: `\(.repository)`",
      "",
      "Run: `\(.run_id)` attempt `\(.run_attempt)`",
      "",
      "Total jobs: **\(.total_jobs)**",
      "",
      "Completed jobs with duration: **\(.completed_jobs)**",
      "",
      "Outcomes: \($outcome_counts)",
      "",
      "## Completed Jobs by Duration",
      ""
    ]
    + job_table($job_rows; "_No completed jobs with start and completion timestamps._")
    + [
      "",
      "## Longest Steps",
      ""
    ]
    + step_table($step_rows; "_No completed steps with start and completion timestamps._")
    + [
      "",
      "## Longest Test-Like Steps",
      ""
    ]
    + step_table($test_step_rows; "_No completed test-like steps found._")
    + [
      "",
      "## Failed, Cancelled, Or Skipped Jobs",
      ""
    ]
    + problem_table($problem_rows)
    | .[]
' "$report_json" > "$report_md"

echo "Wrote $report_md"
echo "Wrote $report_json"
echo "Wrote $report_csv"
