#!/usr/bin/env sh

curl -s http://localhost:8000/openapi.json | jq > openapi.json

# Removes `-v2` suffix so that API client library exposes latest API version without version suffix
# Fixes duplicate version suffixes in operationIds like "_v1-v1" and remove "-v2" suffixes
jq '
  walk(
    if type == "object" and has("operationId") then
      .operationId = (.operationId | gsub("_v1-v1$"; "_v1") | gsub("-v2$"; ""))
    else
      .
    end
  )
' openapi.json > openapi.processed.json && mv openapi.processed.json openapi.json

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