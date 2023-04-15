#!/usr/bin/env sh

curl -s http://localhost:8000/openapi.json | jq > openapi.json

sed -i 's/"schema": {}/"schema": {"type": "object"}/g' openapi.json
sed -i 's/"items": {}/"items": { "type": "object" }/g' openapi.json
sed -i -e 's/"title": "Collection Name"/"title": "Collection Name","type": "string"/g' openapi.json

openapi-generator-plus -c config.yml

sed -i -e '/import "whatwg-fetch";/d' -e 's/window.fetch/fetch/g' src/generated/runtime.ts

rm openapi.json
