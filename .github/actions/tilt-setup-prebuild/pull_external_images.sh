#!/usr/bin/env bash

# This is a simple script to gather all external images referenced in Kubernetes manifests and then pull them in parallel.

set -euo pipefail

k8s_dir="k8s"

# Collect literal image references and ignore templated values
mapfile -t images < <(
  grep -RhoE '^[[:space:]]*image:[[:space:]]*[[:graph:]]+' "$k8s_dir" |
  grep -v '{{' |
  sed -E 's/.*image:[[:space:]]*//' |
  tr -d '"' |
  # chroma-postgres appears to be an external image ref, but it's a custom image.
  # It's just the base postgres image with a single file copied in (k8s/test/postgres/Dockerfile) so it's ok to build during `tilt ci`.
  grep -vi 'chroma-postgres' |
  # The load service appears in k8s/test and is not Helm-templated, so we must exclude it here.
  grep -vi 'load-service' |
  sort -u
)

(( ${#images[@]} )) || { echo "No literal images found, nothing to pull."; exit 0; }

# Build a temporary docker-compose file
tmpfile=$(mktemp)
{
  echo "services:"
  for i in "${!images[@]}"; do
    echo "  img$i:"
    echo "    image: \"${images[$i]}\""
  done
} > "$tmpfile"

echo "Generated compose file:"
cat "$tmpfile"

# Pull all images concurrently
docker compose -f "$tmpfile" pull --parallel
