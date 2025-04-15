---
{
  "id": "introduction",
  "title": "Introduction",
  "section": "Overview",
  "order": 0
}
---

# Chroma

**Chroma is the open-source AI application database**. Chroma makes it easy to build LLM apps by making knowledge, facts, and skills pluggable for LLMs.

{% Banner type="tip" %}
New to Chroma? Check out the [getting started guide](./getting-started)
{% /Banner %}

![Chroma Computer](/computer.png)

Chroma gives you everything you need for retrieval:

- Store embeddings and their metadata
- Vector search
- Full-text search
- Document storage
- Metadata filtering
- Multi-modal retrieval

Chroma runs as a server and provides `Python` and `JavaScript/TypeScript` client SDKs. Check out the [Colab demo](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing) (yes, it can run in a Jupyter notebook).

Chroma is licensed under [Apache 2.0](https://github.com/chroma-core/chroma/blob/main/LICENSE)

### Python
In Python, Chroma can run in a python script or as a server. Install Chroma with

```shell
pip install chromadb
```

### JavaScript
In JavaScript, use the Chroma JS/TS Client to connect to a Chroma server. Install Chroma with your favorite package manager:

{% TabbedUseCaseCodeBlock language="Terminal" %}

{% Tab label="yarn" %}
```terminal
yarn add chromadb chromadb-default-embed
```
{% /Tab %}

{% Tab label="npm" %}
```terminal
npm install --save chromadb chromadb-default-embed
```
{% /Tab %}

{% Tab label="pnpm" %}
```terminal
pnpm install chromadb chromadb-default-embed
```
{% /Tab %}

{% /TabbedUseCaseCodeBlock %}


Continue with the full [getting started guide](./getting-started).


***

## Language Clients

| Language      | Client                                                                                                                   |
|---------------|--------------------------------------------------------------------------------------------------------------------------|
| Python        | [`chromadb`](https://pypistats.org/packages/chromadb) (by Chroma)                                                        |
| Javascript    | [`chromadb`](https://www.npmjs.com/package/chromadb) (by Chroma)                                                         |
| Ruby          | [from @mariochavez](https://github.com/mariochavez/chroma)                                                               |
| Java          | [from @t_azarov](https://github.com/amikos-tech/chromadb-java-client)                                                    |
| Go            | [from @t_azarov](https://github.com/amikos-tech/chroma-go)                                                               |
| C#/.NET       | [from @cincuranet, @ssone95, @microsoft](https://github.com/ssone95/ChromaDB.Client)                                     |
| Rust          | [from @Anush008](https://crates.io/crates/chromadb)                                                                      |
| Elixir        | [from @3zcurdia](https://hex.pm/packages/chroma/)                                                                        |
| Dart          | [from @davidmigloz](https://pub.dev/packages/chromadb)                                                                   |
| PHP           | [from @CodeWithKyrian](https://github.com/CodeWithKyrian/chromadb-php)                                                   |
| PHP (Laravel) | [from @HelgeSverre](https://github.com/helgeSverre/chromadb)                                                             |
| Clojure       | [from @levand](https://github.com/levand/clojure-chroma-client)                                                          |
| R             | [from @cynkra](https://cynkra.github.io/rchroma/)                                                                       |
| C++           | [from @BlackyDrum](https://github.com/BlackyDrum/chromadb-cpp)                                                           |


{% br %}{% /br %}

We welcome [contributions](/markdoc/content/docs/overview/contributing.md) for other languages!

