# Use Chroma with Ejentum cognitive harness

[Ejentum](https://ejentum.com) is a cognitive harness API that returns task-matched reasoning scaffolds for one of four modes (`reasoning`, `code`, `anti-deception`, `memory`). The `memory` mode is the relevant one here: it returns perception scaffolds (what to attend to, what to suppress, what to verify) for *deciding* what to store and *scoring* what to retrieve, rather than storing or retrieving anything itself.

That makes it a natural complement to Chroma. Chroma handles the storage and vector recall; Ejentum's memory harness shapes the human-readable decisions around what deserves storage and what's actually relevant once retrieved.

This recipe covers two patterns:

1. **Pre-storage filter.** Before calling `collection.add(...)`, retrieve a perception scaffold and use it to decide whether the candidate document deserves indexing. Keeps the index lean.
2. **Post-recall scoring.** After `collection.query(...)` returns top-k by similarity, use a different scaffold to assess which results actually fit the user's current intent before passing them to your downstream LLM. Similarity is not the same as relevance.

Both patterns are plain Python composition over Chroma's public client and Ejentum's public REST gateway, so they slot into any pipeline that already uses Chroma.

## Setup

Get an Ejentum API key at [ejentum.com/dashboard](https://ejentum.com/dashboard). Free and paid tiers are available.

```bash
pip install chromadb openai requests
export OPENAI_API_KEY=sk-...
export EJENTUM_API_KEY=ek_...
```

The runnable example is in `harness_around_chroma.py` in this directory. The two patterns it demonstrates are also shown inline below.

## Pattern 1: Pre-storage filter

Index every utterance and you get a noisy collection. Use the memory harness to gate `collection.add` so only signals worth long-term storage land in the index.

```python
from openai import OpenAI
import chromadb
import requests, os

EJENTUM_URL = "https://ejentum-main-ab125c3.zuplo.app/logicv1/"

def harness(query: str, mode: str = "memory") -> str:
    r = requests.post(
        EJENTUM_URL,
        json={"query": query, "mode": mode},
        headers={"Authorization": f"Bearer {os.environ['EJENTUM_API_KEY']}"},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()[0].get(mode, "")

def should_index(candidate: str, client: OpenAI) -> tuple[bool, str]:
    scaffold = harness(
        f"I am deciding whether to commit this statement to a vector store "
        f"for long-term recall. Sharpen: signal vs noise, specific vs generic, "
        f"future-recall value. Statement: {candidate!r}",
        mode="memory",
    )
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content":
                "Apply the perception scaffold below, then answer "
                "INDEX or SKIP on the first line and a short reason on "
                f"the second.\n\n[SCAFFOLD]\n{scaffold}\n[END]"},
            {"role": "user", "content": candidate},
        ],
        temperature=0,
    )
    verdict, _, reason = completion.choices[0].message.content.strip().partition("\n")
    return verdict.strip().upper() == "INDEX", reason.strip()
```

## Pattern 2: Post-recall scoring

`collection.query` returns top-k by similarity. That's an upstream signal of "near," not a downstream judgement of "useful." The harness sharpens the second.

```python
def rerank_with_harness(question: str, hits: list[str], client: OpenAI) -> list[str]:
    if not hits:
        return hits
    scaffold = harness(
        f"I am scoring vector-store hits for relevance to a current question. "
        f"Sharpen: actual fit vs lexical match, recency-bias risk, whether each "
        f"hit shifts the answer. Question: {question!r}",
        mode="memory",
    )
    enumerated = "\n".join(f"[{i}] {h}" for i, h in enumerate(hits))
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content":
                "Apply the perception scaffold below, then list the indices "
                "of hits that materially help answer the question. "
                f"Comma-separated, indices only.\n\n[SCAFFOLD]\n{scaffold}\n[END]"},
            {"role": "user", "content": f"Question: {question}\n\nHits:\n{enumerated}"},
        ],
        temperature=0,
    )
    chosen = {int(s.strip()) for s in completion.choices[0].message.content.split(",") if s.strip().isdigit()}
    return [h for i, h in enumerate(hits) if i in chosen]
```

## Where this fits in your pipeline

Both gates are optional and composable with the rest of Chroma's API. They cost one Ejentum call (a few hundred milliseconds) plus one short LLM call each. Run the pre-storage filter once per candidate; run the post-recall reranker once per user question after `collection.query`.

For high-volume write paths, batch the gate per session rather than per utterance, or sample it. The post-recall reranker is cheap enough to run on every retrieval since both inputs are short.

See `harness_around_chroma.py` for an end-to-end runnable demo that exercises both patterns against a small Chroma collection.
