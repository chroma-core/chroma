#!/usr/bin/env bash
# Determines which test suites to run based on changed paths.
# Uses a whitelist approach: run a suite only when its paths change.
# Reads filter outputs from env vars (set by the calling workflow).
# Writes tests-to-run to GITHUB_OUTPUT.
#
# Env vars (from dorny/paths-filter):
#   FILTER_JS_CLIENT, FILTER_RUST, FILTER_PYTHON, FILTER_GO
#   FILTER_CI_INFRA - when true, run all tests (safety override)

set -euo pipefail

# Whitelist: run each suite when its path filter matches (see pr.yml for which paths trigger which).
TESTS_TO_RUN=()
[[ "${FILTER_RUST:-false}" == "true" ]] && TESTS_TO_RUN+=("rust")
[[ "${FILTER_PYTHON:-false}" == "true" ]] && TESTS_TO_RUN+=("python")
[[ "${FILTER_GO:-false}" == "true" ]] && TESTS_TO_RUN+=("go")
[[ "${FILTER_JS_CLIENT:-false}" == "true" ]] && TESTS_TO_RUN+=("js-client")

# Deduplicate (e.g. rust paths are in python and js-client filters too)
UNIQUE=()
for s in "${TESTS_TO_RUN[@]}"; do
  if [[ " ${UNIQUE[*]} " != *" $s "* ]]; then
    UNIQUE+=("$s")
  fi
done
TESTS_TO_RUN=("${UNIQUE[@]}")

# If CI/infra changed, run all tests (safety override)
if [[ "${FILTER_CI_INFRA:-false}" == "true" ]]; then
  echo "CI/infra paths changed, running all tests"
  TESTS_TO_RUN=("python" "rust" "js-client" "go")
elif [[ ${#TESTS_TO_RUN[@]} -eq 0 ]]; then
  echo "No path filters matched, skipping tests"
  echo "tests-to-run=[]" >> "$GITHUB_OUTPUT"
  exit 0
fi

# Output as JSON array
printf -v joined '"%s",' "${TESTS_TO_RUN[@]}"
echo "tests-to-run=[${joined%,}]" >> "$GITHUB_OUTPUT"
