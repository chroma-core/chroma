#!/usr/bin/env bash
# Determines which test suites to run based on changed paths.
# Uses a whitelist approach: run a suite only when its paths change.
# Reads filter outputs from env vars (set by the calling workflow).
# Writes docs-only and tests-to-run to GITHUB_OUTPUT.
#
# Env vars (from dorny/paths-filter):
#   FILTER_DOCS, FILTER_OUTSIDE_DOCS
#   FILTER_JS_CLIENT, FILTER_RUST, FILTER_PYTHON, FILTER_GO
#   FILTER_CI_INFRA - when true and no other filters matched, run all tests

set -euo pipefail

# If changes are docs-only (changes in docs but not outside docs), skip all tests
if [[ "${FILTER_DOCS:-false}" == "true" && "${FILTER_OUTSIDE_DOCS:-true}" == "false" ]]; then
  echo "Only documentation changes detected, skipping all tests"
  echo "docs-only=true" >> "$GITHUB_OUTPUT"
  echo "tests-to-run=[]" >> "$GITHUB_OUTPUT"
  exit 0
fi

echo "docs-only=false" >> "$GITHUB_OUTPUT"

# Whitelist: run each suite only when its paths changed.
# Core (rust) triggers downstream: Python uses Rust bindings, JS client runs
# integration tests against the Rust server, so rust changes run rust + python + js-client.
TESTS_TO_RUN=()
if [[ "${FILTER_RUST:-false}" == "true" ]]; then
  TESTS_TO_RUN+=("rust" "python" "js-client")
fi
[[ "${FILTER_PYTHON:-false}" == "true" ]] && TESTS_TO_RUN+=("python")
[[ "${FILTER_GO:-false}" == "true" ]] && TESTS_TO_RUN+=("go")
[[ "${FILTER_JS_CLIENT:-false}" == "true" ]] && TESTS_TO_RUN+=("js-client")

# Deduplicate (rust trigger can add python/js-client; direct filter can add them again)
UNIQUE=()
for s in "${TESTS_TO_RUN[@]}"; do
  if [[ " ${UNIQUE[*]} " != *" $s "* ]]; then
    UNIQUE+=("$s")
  fi
done
TESTS_TO_RUN=("${UNIQUE[@]}")

# If no path filters matched, run all tests when CI/infra changed (safety)
if [[ ${#TESTS_TO_RUN[@]} -eq 0 ]]; then
  if [[ "${FILTER_CI_INFRA:-false}" == "true" ]]; then
    echo "CI/infra paths changed, running all tests"
    TESTS_TO_RUN=("python" "rust" "js-client" "go")
  else
    echo "No path filters matched, skipping tests"
    echo "tests-to-run=[]" >> "$GITHUB_OUTPUT"
    exit 0
  fi
fi

# Output as JSON array
printf -v joined '"%s",' "${TESTS_TO_RUN[@]}"
echo "tests-to-run=[${joined%,}]" >> "$GITHUB_OUTPUT"
