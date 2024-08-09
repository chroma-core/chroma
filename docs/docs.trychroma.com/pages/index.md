---
title: Chroma
---

<!-- ![](/img/hrm4.svg) -->

Chroma is **the world’s most popular open-source vector database**.

> 💡 **New to Chroma?** Get up and running in less than 60 seconds.
>
> [🔑 Getting Started](/getting-started)

Chroma is AI-native, meaning it has been designed from the ground up to power AI applications. Chroma makes it simple for you to store and retrieve data that is most relevant to your queries, ensuring your LLM has the information it needs to provide accurate and context-aware answers.

Chroma gives you the tools for: (* note - could link to documentation here?)
- **Vector search** - Use embeddings to search for similar documents or data points
- **Full-text search** - Search stored documents based on their content
- **Metadata search** - Search stored documents based on their metadata
- **Combined Search** - Find similar documents subject to full-text and metadata constraints

Chroma prioritizes:
- **Simplicity and developer productivity** – Chroma's default functionality lets you get up and running quickly with minimal code.
- **Portability** – Chroma can run locally on your machine in many different languages, with a hosted version coming soon
- **Efficiency** - Chroma is lightweight and fast!

Chroma runs where you need it: (* note - could link to example here?)
- in a Jupyter notebook
- in a Python script
- as a single-node server
- as a distributed cluster

Want to find out more? See our Guides explaining:
- How Chroma Works
- Setting Up Clients
- Working with Collections
- and more

Chroma is licensed under [Apache 2.0](https://github.com/chroma-core/chroma/blob/main/LICENSE)

[![Discord](https://img.shields.io/discord/1073293645303795742?cacheSeconds=3600&style=social&logo=discord&logoColor=000000&label=&nbsp;)](https://discord.gg/MMeYNTmh3x)
{% br %}{% /br %}
[![GitHub stars](https://img.shields.io/github/stars/chroma-core/chroma.svg?style=social&label=Star&maxAge=2400)](https://GitHub.com/chroma-core/chroma/stargazers/)


***

## Quick install

### Python
In Python, Chroma can run in a python script or as a server.

```bash
pip install chromadb
```

### JavaScript
In JavaScript, use the Chroma JS/TS Client to connect to a Chroma server.

{% codetabs customHeader="sh" %}
{% codetab label="yarn" %}
```bash {% codetab=true %}
yarn install chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% codetab label="npm" %}
```bash {% codetab=true %}
npm install --save chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% codetab label="pnpm" %}
```bash {% codetab=true %}
pnpm install chromadb chromadb-default-embed # [!code $]
```
{% /codetab %}
{% /codetabs %}


Continue with the full [getting started guide](./getting-started).


***

### Language Clients

{% special_table %}
{% /special_table %}

|              | client |
|--------------|---------------|
| Python       | ✅ [`chromadb`](https://pypistats.org/packages/chromadb) (by Chroma)           |
| Javascript   | ✅ [`chromadb`](https://www.npmjs.com/package/chromadb) (by Chroma)          |
| Ruby   | ✅ [from @mariochavez](https://github.com/mariochavez/chroma)           |
| Java | ✅ [from @t_azarov](https://github.com/amikos-tech/chromadb-java-client) |
| Go | ✅ [from @t_azarov](https://github.com/amikos-tech/chroma-go) |
| C#   | ✅ [from @microsoft](https://github.com/microsoft/semantic-kernel/tree/main/dotnet/src/Connectors/Connectors.Memory.Chroma)       |
| Rust  | ✅ [from @Anush008](https://crates.io/crates/chromadb) |
| Elixir  | ✅ [from @3zcurdia](https://hex.pm/packages/chroma/) |
| Dart  | ✅ [from @davidmigloz](https://pub.dev/packages/chromadb) |
| PHP  | ✅ [from @CodeWithKyrian](https://github.com/CodeWithKyrian/chromadb-php) |
| PHP (Laravel)  | ✅ [from @HelgeSverre](https://github.com/helgeSverre/chromadb)                                                            |

{% br %}{% /br %}

We welcome [contributions](/contributing) for other languages!
