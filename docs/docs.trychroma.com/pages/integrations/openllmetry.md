---
title: 🔭 OpenLLMetry
---

[OpenLLMetry](https://www.traceloop.com/openllmetry) provides observability for systems using Chroma. It allows tracing calls to Chroma, OpenAI, and other services.
It gives visibility to query and index calls as well as LLM prompts and completions.
For more information on how to use OpenLLMetry, see the [OpenLLMetry docs](https://www.traceloop.com/docs/openllmetry).

![](/img/openllmetry.png)

### Example

Install OpenLLMetry SDK by running:

```bash
pip install traceloop-sdk
```

Then, initialize the SDK in your application:

```python
from traceloop.sdk import Traceloop

Traceloop.init()
```

### Configuration

OpenLLMetry can be configured to send traces to any observability platform that supports OpenTelemetry - Datadog, Honeycomb, Dynatrace, New Relic, etc. See the [OpenLLMetry docs](https://www.traceloop.com/openllmetry/provider/chroma) for more information.
