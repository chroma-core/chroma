#!/usr/bin/env sh
set -e

curl -s http://localhost:8000/openapi.json | jq > openapi.json

openapi-generator-plus -c config.yml

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' packages/chromadb-core/src/generated/runtime.ts
else
  sed -i -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' packages/chromadb-core/src/generated/runtime.ts
fi

# Add isomorphic-fetch dependency to runtime.ts
echo "import 'isomorphic-fetch';" > temp.txt
cat packages/chromadb-core/src/generated/runtime.ts >> temp.txt
mv temp.txt packages/chromadb-core/src/generated/runtime.ts

rm openapi.json