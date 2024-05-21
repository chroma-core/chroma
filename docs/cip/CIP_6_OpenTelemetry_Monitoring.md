# CIP 6: OpenTelemetry Monitoring

## **Status**

Current status: `Under Discussion`

## **Motivation**

Chroma currently has very little observability, only offering basic logging. Using Chroma in a high-performance production context requires the ability to understand how Chroma is behaving and responding to requests.

## **Public Interfaces**

The changes will affect the following:

- Logging output
- Several new CLI flags

## **Proposed Changes**

We propose to instrument Chroma with [OpenTelemetry](https://opentelemetry.io/docs/instrumentation/python/) (OTel), the most prevalent open-source observability standard. OTel's Python libraries are considered stable for traces and metrics. We will create several layers of observability, configurable with command-line flags.

- Chroma's default behavior will remain the same: events will be logged to the console with configurable severity levels.
- We will add a flag, `--opentelemetry-mode={api, sdk}` to instruct Chroma to export OTel data in either [API or SDK mode](https://stackoverflow.com/questions/72963553/opentelemetry-api-vs-sdk).
- We will add another flag, `--opentelemtry-detail={partial, full}`, to specify the level of detail desired from OTel.
  - With `partial` detail, Chroma's top-level API calls will produce a single span. This mode is suitable for end-users of Chroma who are not intimately familiar with its operation but use it as part of their production system.
  - `full` detail will emit spans for Chroma's sub-operations, enabling Chroma maintainers to monitor performance and diagnose issues.
- For now Chroma's OTel integrations will need to be specified with environment variables. As the [OTel file configuration project](https://github.com/MrAlias/otel-schema/pull/44) matures we will integrate support for file-based OTel configuration.

## **Compatibility, Deprecation, and Migration Plan**

This change adds no new default-on functionality.

## **Test Plan**

Observability logic and output will be tested on both single-node and distributed Chroma to confirm that metrics are exported properly and traces correctly identify parent spans across function and service boundaries.

## **Rejected Alternatives**

### Prometheus metrics

Prometheus metrics offer similar OSS functionality to OTel. However the Prometheus standard is older and belongs to a single open-source project; OTel is designed for long-term cross-compatibility between *all* observability backends. As such, OTel output can easily be ingested by Prometheus users so there is no loss of functionality or compatibility.
