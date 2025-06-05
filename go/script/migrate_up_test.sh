if pgrep -x atlas > /dev/null; then
  echo "Error: Another atlas instance is already running"
  exit 1
fi

atlas schema apply \
  -u "$1" \
  --to file://pkg/log/store/schema/ \
--dev-url "docker://postgres/15/dev" \
--auto-approve
