"""End-to-end demo: Ejentum cognitive harness around a Chroma collection.

Demonstrates two patterns, both as plain Python composition over Chroma's
public client and Ejentum's REST gateway:

1. Pre-storage filter. Use the memory harness to decide whether a
   candidate statement deserves a `collection.add(...)` call.
2. Post-recall scoring. After `collection.query(...)`, use the harness
   as a reranker that flags hits as materially relevant vs. just lexically
   similar.

Run:
    pip install chromadb openai requests
    export OPENAI_API_KEY=sk-...
    export EJENTUM_API_KEY=ek_...    # https://ejentum.com/dashboard
    python harness_around_chroma.py
"""

from __future__ import annotations

import os
import textwrap

import chromadb
import requests
from openai import OpenAI


EJENTUM_URL = "https://ejentum-main-ab125c3.zuplo.app/logicv1/"


def harness(query: str, mode: str = "memory") -> str:
    """Fetch a perception scaffold from the Ejentum REST gateway."""
    r = requests.post(
        EJENTUM_URL,
        json={"query": query, "mode": mode},
        headers={"Authorization": f"Bearer {os.environ['EJENTUM_API_KEY']}"},
        timeout=10,
    )
    r.raise_for_status()
    payload = r.json()
    return payload[0].get(mode, "") if payload else ""


def should_index(candidate: str, openai_client: OpenAI) -> tuple[bool, str]:
    scaffold = harness(
        f"I am deciding whether to commit this statement to a vector store "
        f"for long-term recall. Sharpen: signal vs noise, specific vs generic, "
        f"future-recall value. Statement: {candidate!r}",
        mode="memory",
    )
    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "Apply the perception scaffold below, then answer INDEX or "
                    "SKIP on the first line and a short reason on the second.\n\n"
                    f"[SCAFFOLD]\n{scaffold}\n[END]"
                ),
            },
            {"role": "user", "content": candidate},
        ],
        temperature=0,
    )
    text = completion.choices[0].message.content.strip()
    verdict, _, reason = text.partition("\n")
    return verdict.strip().upper() == "INDEX", reason.strip()


def rerank_with_harness(
    question: str, hits: list[str], openai_client: OpenAI
) -> list[str]:
    if not hits:
        return hits
    scaffold = harness(
        f"I am scoring vector-store hits for relevance to a current question. "
        f"Sharpen: actual fit vs lexical match, recency-bias risk, whether each "
        f"hit shifts the answer. Question: {question!r}",
        mode="memory",
    )
    enumerated = "\n".join(f"[{i}] {h}" for i, h in enumerate(hits))
    completion = openai_client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "Apply the perception scaffold below, then list the indices "
                    "of memories that materially help answer the question. "
                    "Output a comma-separated list of indices and nothing else.\n\n"
                    f"[SCAFFOLD]\n{scaffold}\n[END]"
                ),
            },
            {
                "role": "user",
                "content": f"Question: {question}\n\nMemories:\n{enumerated}",
            },
        ],
        temperature=0,
    )
    chosen = {
        int(s.strip())
        for s in completion.choices[0].message.content.split(",")
        if s.strip().isdigit()
    }
    return [h for i, h in enumerate(hits) if i in chosen]


def main() -> None:
    openai_client = OpenAI()
    chroma_client = chromadb.Client()
    collection = chroma_client.get_or_create_collection("alice_notes")

    candidates = [
        "I prefer Python over TypeScript for new backend services.",
        "Today is Friday.",
        "My team uses GitHub Actions for CI, and we deploy twice a week.",
        "The conference room is on the 3rd floor.",
        "I am moving away from REST and toward gRPC for internal services.",
    ]

    print("== Pre-storage filter ==")
    for candidate in candidates:
        keep, why = should_index(candidate, openai_client)
        if keep:
            collection.add(documents=[candidate], ids=[f"id-{len(collection.get()['ids'])}"])
            print(f"INDEXED  {candidate!r}\n         {textwrap.shorten(why, 120)}")
        else:
            print(f"SKIPPED  {candidate!r}\n         {textwrap.shorten(why, 120)}")

    print()
    print("== Post-recall scoring ==")
    question = "What stack does my team use?"
    raw = collection.query(query_texts=[question], n_results=5)
    hits = raw["documents"][0]
    print(f"Raw chroma hits (top {len(hits)} by similarity):")
    for h in hits:
        print(f"  - {h!r}")

    relevant = rerank_with_harness(question, hits, openai_client)
    print()
    print(f"After harness rerank ({len(relevant)} kept):")
    for h in relevant:
        print(f"  + {h!r}")


if __name__ == "__main__":
    main()
