---
slug: /integrations/langtrace
title: 📊 Langtrace
---

[Langtrace](www.langtrace.ai) is an open-source observability tool which enables developers to trace, evaluate, manage prompts and datasets, and debug issues related to an LLM application’s performance. It creates open telemetry standard traces for Chroma which helps with observability and works with any observability client.

![](/img/langtrace.png)

Key features include:

- Detailed traces and logs
- Real-time monitoring of key metrics including accuracy, evaluations, usage, costs, and latency
- Integrations for the most popular frameworks, vector databases, and LLMs including Langchain, LllamaIndex, OpenAI, Anthropic, Pinecone, Chroma and Cohere.
- Self-hosted or using Langtrace cloud

| [Docs](https://docs.langtrace.ai/introduction) | [Github](https://github.com/Scale3-Labs/langtrace) |

### Installation

- Signup for [Langtrace cloud](https://langtrace.ai/signup) to get an API key

#### Install the SDK on your project:

- **Python**: Install the Langtrace SDK using pip

```bash
pip install langtrace-python-sdk
```

- **Typescript**: Install the Langtrace SDK using npm

```bash
npm i @langtrase/typescript-sdk
```

#### Initialize the SDK in your project:

- **Python**:

```python Python
from langtrace_python_sdk import langtrace

langtrace.init(api_key = '<LANGTRACE_API_KEY>')
```

- **Typescript**:

```javascript
// Must precede any llm module imports
import * as Langtrace from "@langtrase/typescript-sdk";

Langtrace.init({ api_key: "<LANGTRACE_API_KEY>" });
```



### Configuration

Langtrace is adaptable and can be configured to transmit traces to any observability platform compatible with OpenTelemetry, such as Datadog, Honeycomb, Dynatrace, New Relic, among others. For more details on setup and options, consult the [Langtrace docs](https://www.langtrace.ai).