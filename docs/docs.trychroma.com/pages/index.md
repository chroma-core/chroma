---
title: Chroma
---

**Chroma is the AI-native open-source vector database**. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

New to Chroma? [🔑 Getting Started](./getting-started).

[![Discord](https://img.shields.io/discord/1073293645303795742?cacheSeconds=3600)](https://discord.gg/MMeYNTmh3x)
{% br %}{% /br %}
[![GitHub stars](https://img.shields.io/github/stars/chroma-core/chroma.svg?style=social&label=Star&maxAge=2400)](https://GitHub.com/chroma-core/chroma/stargazers/)


***


![](/img/hrm4.svg)

{% br %}{% /br %}

Chroma gives you the tools to:

- store embeddings and their metadata
- embed documents and queries
- search embeddings

Chroma prioritizes:

- simplicity and developer productivity
- it also happens to be very quick

Chroma runs as a server and provides 1st party `Python` and `JavaScript/TypeScript` client SDKs. Check out the [Colab demo](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing). (yes, it can run in a Jupyter notebook 😄)

Chroma is licensed under [Apache 2.0](https://github.com/chroma-core/chroma/blob/main/LICENSE)

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
| PHP (Laravel)  | ✅ [from @HelgeSverre](https://github.com/helgeSverre/chromadb) |
| Clojure | ✅ [from @levand](https://github.com/levand/clojure-chroma-client) |                                                         |

{% br %}{% /br %}

We welcome [contributions](/contributing) for other languages!
