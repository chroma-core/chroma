set -euo pipefail

k8s_dir="k8s"

# Collect literal image references and ignore templated values
mapfile -t images < <(
  grep -RhoE '^[[:space:]]*image:[[:space:]]*[[:graph:]]+' "$k8s_dir" |
  grep -v '{{' |
  sed -E 's/.*image:[[:space:]]*//' |
  tr -d '"' |
  grep -vi 'chroma-postgres' |                 # EXCLUDE chroma-postgres
  grep -vi 'load-service' |                 # EXCLUDE chroma-postgres
  sort -u
)

(( ${#images[@]} )) || { echo "No literal images found – nothing to pull."; exit 0; }

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
