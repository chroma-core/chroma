## Demo in browser

First, update Chroma's config to allow the `localhost:3000` origin for CORS.

For example, you could start Chroma with

```bash
CHROMA_SERVER_CORS_ALLOW_ORIGINS='["http://localhost:3000"]' chroma run
```

Then, in this folder:

1. `pnpm i`
2. `pnpm dev`
3. visit `localhost:3000`
