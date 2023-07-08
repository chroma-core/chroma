## Demo in browser

Update your settings to add `localhost:3000` to `chroma_server_cors_allow_origins`.

For example in `docker-compose.yml`

```
environment:
      - CHROMA_DB_IMPL=clickhouse
      - CLICKHOUSE_HOST=clickhouse
      - CLICKHOUSE_PORT=8123
      - CHROMA_SERVER_CORS_ALLOW_ORIGINS=["http://localhost:3000"]
```

1. `yarn dev`
2. visit `localhost:3000`
