atlas schema apply \
  -u "$1" \
  --to file://pkg/log/store/schema/ \
--dev-url "docker://postgres/15/dev" \
--auto-approve
