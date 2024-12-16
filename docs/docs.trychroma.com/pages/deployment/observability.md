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
- `CHROMA_OTEL_COLLECTION_HEADERS`: Headers to use when sending observability data. Often used to send API and app keys. For example `{"x-honeycomb-team": "abc"}`.
- `CHROMA_OTEL_GRANULARITY`: A value from the [OpenTelemetryGranularity enum](https://github.com/chroma-core/chroma/tree/main/chromadb/telemetry/opentelemetry/__init__.py). Specifies how detailed tracing should be.

We also have dedicated observability guides for various deployments:
* [Docker](/deployment/docker#observability-with-docker)
* [AWS](/deployment/aws#observability-with-AWS)
* [GCP](/deployment/gcp#observability-with-GCP)
* [Azure](/deployment/azure#observability-with-Azure)

## Client (SDK) Observability

Several observability platforms offer built-in integrations for Chroma, allowing you to monitor your application's interactions with the Chroma server:
- [OpenLLMetry Integration](/integrations/openllmetry).
- [OpenLIT Integration](/integrations/openlit).
