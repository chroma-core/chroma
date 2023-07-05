#!/usr/bin/env sh

# curl -s http://localhost:8000/openapi.json | jq > openapi.json
curl -s http://localhost:8000/openapi.json | python -c "import sys, json; print(json.dumps(json.load(sys.stdin), indent=2))" > openapi.json

if [[ "$OSTYPE" == "darwin"* ]]; then
  # macOS
  sed -i '' 's/"schema": {}/"schema": {"type": "object"}/g' openapi.json
  sed -i '' 's/"items": {}/"items": { "type": "object" }/g' openapi.json
  sed -i '' -e 's/"title": "Collection Name"/"title": "Collection Name","type": "string"/g' openapi.json
else
  # Linux
  sed -i 's/"schema": {}/"schema": {"type": "object"}/g' openapi.json
  sed -i 's/"items": {}/"items": { "type": "object" }/g' openapi.json
  sed -i -e 's/"title": "Collection Name"/"title": "Collection Name","type": "string"/g' openapi.json
fi

openapi-generator-plus -c config.yml

if [[ "$OSTYPE" == "darwin"* ]]; then
  sed -i '' -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' src/generated/runtime.ts
else
  sed -i -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' src/generated/runtime.ts
fi

# Add isomorphic-fetch dependency to runtime.ts
echo "import 'isomorphic-fetch';" > temp.txt
cat src/generated/runtime.ts >> temp.txt
mv temp.txt src/generated/runtime.ts

rm openapi.json
