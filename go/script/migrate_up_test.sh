atlas schema apply \
  -u "$1" \
  --to file://database/log/schema/ \
--dev-url "docker://postgres/15/dev" \
--auto-approve
