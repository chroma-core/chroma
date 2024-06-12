---
title: "🛣️ Roadmap"
---

{% note type="default" title="Last Updated" %}
`May 20, 2024`
{% /note %}

The goal of this doc is to align *core* and *community* efforts for the project and to share what's in store for this year!

**Sections**
- What is the core Chroma team working on right now?
- What will Chroma prioritize over the next 6mo?
- What areas are great for community contributions?

## What is the core Chroma team working on right now?

- 🌩️ Standing up that distributed system as a managed service (aka "Hosted Chroma" - [sign up for waitlist](https://airtable.com/shrOAiDUtS2ILy5vZ)!)

## What did the Chroma team just complete?

Features like:
- *New* - [Chroma 0.4](https://www.trychroma.com/blog/chroma_0.4.0) - our first production-oriented release
- 🐍 A more minimal python-client only build target
- ✋ Google PaLM embedding support
- 🎣 OpenAI ChatGPT Retrieval Plugin

## What will Chroma prioritize over the next 6mo?

**Next Milestone: ☁️ Launch Hosted Chroma**

**Areas we will invest in**

Not an exhaustive list, but these are some of the core team’s biggest priorities over the coming few months. Use caution when contributing in these areas and please check-in with the core team first.

- ⏩ **Workflow**: Building tools for answer questions like: what embedding model should I use? And how should I chunk up my documents?
- 🌌 **Visualization**: Building visualization tool to give developers greater intuition embedding spaces
- 🔀 **Query Planner**: Building tools to enable per-query and post-query transforms
- 🔧 **Developer experience**: Extending Chroma into a CLI
- 📦 **Easier Data Sharing**: Working on formats for serialization and easier data sharing of embedding Collections
- 🔍 **Improving recall**: Fine-tuning embedding transforms through human feedback
- 🧠 **Analytical horsepower**: Clustering, deduplication, classification and more

## What areas are great for community contributions?

This is where you have a lot more free reign to contribute (without having to sync with us first)!

If you're unsure about your contribution idea, feel free to chat with us (@chroma) in the `#general` channel in [our Discord](https://discord.gg/rahcMUU5XV)! We'd love to support you however we can.

### ⚙️ Example Templates

We can always use [more integrations](https://docs.trychroma.com/integrations) with the rest of the AI ecosystem. Please let us know if you're working on one and need help!

Other great starting points for Chroma (please send PRs for more [here](https://github.com/chroma-core/docs/tree/swyx/addRoadmap/docs)):
- [Google Colab](https://colab.research.google.com/drive/1QEzFyqnoFxq7LUGyP1vzR4iLt9PpCDXv?usp=sharing)
- [Replit Template](https://replit.com/@swyx/BasicChromaStarter?v=1)

For those integrations we do have, like `LangChain` and `LlamaIndex`, we do always want more tutorials, demos, workshops, videos, and podcasts (we've done some pods [on our blog](https://trychroma.com/blog)).

### 📦 Example Datasets

It doesn’t make sense for developers to embed the same information over and over again with the same embedding model.

We'd like suggestions for:

- "small" (<100 rows)
- "medium" (<5MB)
- "large" (>1GB)

datasets for people to stress test Chroma in a variety of scenarios.

### ⚖️ Embeddings Comparison

Chroma does ship with Sentence Transformers by default for embeddings, but we are otherwise unopinionated about what embeddings you use. Having a library of information that has been embedded with many models, alongside example query sets would make it much easier for empirical work to be done on the effectiveness of various models across different domains.

- [Preliminary reading on Embeddings](https://towardsdatascience.com/neural-network-embeddings-explained-4d028e6f0526?gi=ee46baab0d8f)
- [Huggingface Benchmark of a bunch of Embeddings](https://huggingface.co/blog/mteb)
- [notable issues with GPT3 Embeddings](https://twitter.com/Nils_Reimers/status/1487014195568775173) and alternatives to consider

### ⚗️ Experimental Algorithms

If you have a research background, please consider adding to our `ExperimentalAPI`s. For example:

- Projections (t-sne, UMAP, the new hotness, the one you just wrote) and Lightweight visualization
- Clustering (HDBSCAN, PCA)
- Deduplication
- Multimodal (CLIP)
- Fine-tuning manifold with human feedback [eg](https://github.com/openai/openai-cookbook/blob/main/examples/Customizing_embeddings.ipynb)
- Expanded vector search (MMR, Polytope)
- Your research

### 🧑‍💻️ Additional Client SDKs

We will be happy to work with people maintaining additional client SDKs as part of the community. Specifically:

- Ruby
- Clojure
- Elixir

You can find the REST OpenAPI spec at `localhost:8000/openapi.json` when the backend is running.

Please [reach out](https://discord.gg/MMeYNTmh3x) and talk to us before you get too far in your projects so that we can offer technical guidance/align on roadmap.
