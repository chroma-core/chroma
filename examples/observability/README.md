# Observability

## Local Observability Stack

To run the Chroma with local observability stack (OpenTelemetry + Zipkin + Zipkin Storage - MariaDB/MySQL),
run the following command from the root of the repository:

```bash
docker compose -f examples/observability/docker-compose.local-observability.yml up --build -d
```
