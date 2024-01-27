# Chroma Full Observability Primer

## Running

> Note: This is an example and is not meant to be exhaustive or production-ready.

```bash
docker compose -f examples/observability/full-observability/docker-compose.yaml up --build -d
```

Navigate to Grafana on `http://localhost:3000/` and login with `admin:admin` (you can skip the change of password).

Under dashboards, you will find the `Chroma Basic` dashboard.

> Note: The dashboard is read-only, if you need to modify it, you can save it as a new dashboard and modify the new one.

## Stopping

```bash
docker compose -f examples/observability/full-observability/docker-compose.yaml down --volumes --rmi
```

## Dashboard Examples

**Core Metrics:**

![img.png](img.png)

**Network:**

![img_1.png](img_1.png)

**Memory and CPU:**

![img_2.png](img_2.png)

**Logs:**

![img_3.png](img_3.png)
