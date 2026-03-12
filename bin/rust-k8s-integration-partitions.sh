#!/usr/bin/env bash
set -euo pipefail

# Run the Rust k8s integration nextest profiles with the same partitioning
# strategy used in the existing GitHub Actions matrix.
#
# Profiles:
# - ci_k8s_integration      (partition_method=hash)
# - ci_k8s_integration_slow (partition_method=count)
#
# Partitions:
# - 1/2 and 2/2 for each profile.

profiles=("ci_k8s_integration" "ci_k8s_integration_slow")

for profile in "${profiles[@]}"; do
  case "$profile" in
    ci_k8s_integration)
      partition_method="hash"
      ;;
    ci_k8s_integration_slow)
      partition_method="count"
      ;;
    *)
      echo "Unknown profile: ${profile}" >&2
      exit 1
      ;;
  esac

  for partition in 1 2; do
    echo "Running profile=${profile} partition=${partition_method}:${partition}/2"
    cargo nextest run \
      --profile "${profile}" \
      --partition "${partition_method}:${partition}/2" \
      --no-tests warn
  done
done

