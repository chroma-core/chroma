---
title: "👀 Observability"
---

## Backend Observability

Chroma is instrumented with [OpenTelemetry](https://opentelemetry.io/) hooks for observability.

{% note type="default" title="Telemetry vs Observability" %}
"[Telemetry](/telemetry)" refers to anonymous product usage statistics we collect. "Observability" refers to metrics, logging, and tracing which can be used by anyone operating a Chroma deployment. Observability features listed on this page are **never** sent back to Chroma; they are for end-users to better understand how their Chroma deployment is behaving.
{% /note %}

### Available Observability

Chroma currently only exports OpenTelemetry [traces](https://opentelemetry.io/docs/concepts/signals/traces/). Traces allow a Chroma operator to understand how requests flow through the system and quickly identify bottlenecks.

### Configuration

Tracing is configured with four environment variables:

- `CHROMA_OTEL_COLLECTION_ENDPOINT`: where to send observability data. Example: `api.honeycomb.com`.
- `CHROMA_OTEL_SERVICE_NAME`: Service name for OTel traces. Default: `chromadb`.
- `CHROMA_OTEL_COLLECTION_HEADERS`: Headers to use when sending observability data. Often used to send API and app keys.
- `CHROMA_OTEL_GRANULARITY`: A value from the [OpenTelemetryGranularity enum](https://github.com/chroma-core/chroma/tree/main/chromadb/telemetry/opentelemetry/__init__.py). Specifies how detailed tracing should be.

## Local Observability Stack (🐳👀📚)

Chroma also comes with a local observability stack. The stack is composed of Chroma Server (the one you know and ❤️), [OpenTelemetry Collector](https://github.com/open-telemetry/opentelemetry-collector), and [Zipkin](https://zipkin.io/).

To start the stack, run from the root of the repo:

```bash
docker compose -f examples/observability/docker-compose.local-observability.yml up --build -d
```

Once the stack is running, you can access Zipkin at http://localhost:9411

{% note type="tip" title="Traces" %}
Traces in Zipkin will start appearing after you make a request to Chroma.
{% /note %}

## Client (SDK) Observability

See 
- [OpenLLMetry Integration](/integrations/openllmetry).
- [OpenLIT Integration](/integrations/openlit).