#!/usr/bin/env sh

# Download the OpenAPI schema
curl -s http://localhost:3000/openapi.json | python -c "import sys, json; print(json.dumps(json.load(sys.stdin), indent=2))" > openapi.json

# Create the output directory if it doesn't exist
mkdir -p src/generated
# Generate TypeScript types using openapi-typescript
npx openapi-typescript openapi.json --output src/schema.d.ts

# Clean up the downloaded schema
rm openapi.json

echo "Successfully generated TypeScript types in src/schema.d.ts"
