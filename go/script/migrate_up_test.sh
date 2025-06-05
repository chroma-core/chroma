LOCK_FILE="/tmp/atlas_migrate.lock"

exec 200>"$LOCK_FILE"
if ! flock -n 200; then
  echo "Waiting for another atlas instance to complete..."
  flock 200
fi

atlas schema apply \
  -u "$1" \
  --to file://pkg/log/store/schema/ \
--dev-url "docker://postgres/15/dev" \
--auto-approve

flock -u 200
