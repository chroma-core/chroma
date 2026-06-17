#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx

from wiki_tilegroups_lib import (
    CLAIM_TYPES,
    HOOK_TYPES,
    CandidateTileRecord,
    ClaimRecord,
    RejectedClaimRecord,
    TileGroupRecord,
    WikiDoc,
    build_tilegroups,
    clamp,
    fallback_claims_for_doc,
    fallback_tile_for_claim,
    normalize_string_list,
    select_docs_for_processing,
    serialize_json,
    should_keep_claim,
    summarize_output,
    utc_now,
    write_json,
    write_jsonl,
)


def coerce_score(value: Any, default: float) -> float:
    try:
        score = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(score):
        return default
    return clamp(score)


def coerce_claim_type(value: Any) -> str:
    claim_type = str(value or "summary").strip()
    if claim_type in CLAIM_TYPES:
        return claim_type
    return "summary"


def coerce_hook_type(value: Any, default: str = "none") -> str:
    hook_type = str(value or default).strip()
    if hook_type in HOOK_TYPES:
        return hook_type
    return default


def coerce_limited_strings(value: Any, limit: int) -> List[str]:
    return normalize_string_list(value)[:limit]


class ChromaCloudWikiClient:
    def __init__(
        self,
        cloud_host: str,
        api_key: str,
        tenant: str,
        database: str,
        timeout: float = 30.0,
    ) -> None:
        self._tenant = tenant
        self._database = database
        self._client = httpx.Client(
            base_url=f"https://{cloud_host}/api/v2",
            headers={"X-Chroma-Token": api_key},
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def get_collection(self, name: str) -> Dict[str, Any]:
        response = self._client.get(
            f"/tenants/{self._tenant}/databases/{self._database}/collections/{name}"
        )
        response.raise_for_status()
        return response.json()

    def get_collection_records(
        self,
        collection_id: str,
        *,
        limit: int,
        offset: int,
        include: Optional[Sequence[str]] = None,
    ) -> Dict[str, Any]:
        payload = {
            "limit": limit,
            "offset": offset,
            "include": list(include or ["metadatas", "documents"]),
        }
        response = self._client.post(
            f"/tenants/{self._tenant}/databases/{self._database}/collections/{collection_id}/get",
            json=payload,
        )
        response.raise_for_status()
        return response.json()

    def fetch_wiki_docs(
        self,
        collection_name: str,
        *,
        page_size: int = 200,
        scan_limit: int = 5000,
    ) -> List[WikiDoc]:
        collection = self.get_collection(collection_name)
        collection_id = collection["id"]
        docs: List[WikiDoc] = []
        offset = 0
        while offset < scan_limit:
            page = self.get_collection_records(
                collection_id,
                limit=min(page_size, scan_limit - offset),
                offset=offset,
            )
            ids = page.get("ids") or []
            if not ids:
                break
            metadatas = page.get("metadatas") or []
            documents = page.get("documents") or []
            for record_id, metadata, document in zip(ids, metadatas, documents):
                docs.append(WikiDoc.from_record(record_id, document, metadata))
            offset += len(ids)
            if len(ids) < page_size:
                break
        return docs


class OpenAIJsonClient:
    def __init__(self, api_key: str, model: str, timeout: float = 60.0) -> None:
        self._model = model
        self._client = httpx.Client(
            base_url="https://api.openai.com/v1",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def json_completion(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        response = self._client.post(
            "/chat/completions",
            json={
                "model": self._model,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            },
        )
        response.raise_for_status()
        payload = response.json()
        content = payload["choices"][0]["message"]["content"]
        return json.loads(content)


def extraction_prompts(current_doc: WikiDoc, previous_doc: Optional[WikiDoc]) -> Tuple[str, str]:
    system_prompt = (
        "You are extracting claims for the Foundation homepage intelligence feed. "
        "Users only click when a tile creates an itch: missing context, a meaningful change, unresolved tension, "
        "a buried decision, a person they know, a divergence between official story and actual work, "
        "near-term impact on their work, or a genuinely novel synthesis that reflects Foundation's value prop. "
        "Return strict JSON with a top-level 'claims' array. Reject generic summaries, titles, headers, "
        "status lines, and restatements of what the document is 'about'. Prefer concrete deductions, workflow implications, "
        "hidden rationale, contradictions, and consequences that would make a teammate click."
    )
    previous_text = previous_doc.document if previous_doc else ""
    previous_meta = {
        "id": previous_doc.id if previous_doc else None,
        "title": previous_doc.title if previous_doc else None,
        "slug": previous_doc.slug if previous_doc else None,
        "version": previous_doc.version if previous_doc else None,
        "updated_at": previous_doc.updated_at if previous_doc else None,
        "document": previous_text[:16000],
    }
    current_meta = {
        "id": current_doc.id,
        "title": current_doc.title,
        "slug": current_doc.slug,
        "version": current_doc.version,
        "updated_at": current_doc.updated_at,
        "source_ids": current_doc.source_ids,
        "categories": current_doc.categories,
        "document": current_doc.document[:16000],
    }
    user_prompt = serialize_json(
        {
            "task": (
                "Extract 3-8 claims. Each claim must include claim_text, claim_type, "
                "hook_type, hook_strength, deduction_score, confidence, evidence, "
                "entities, and people."
            ),
            "allowed_claim_types": [
                "summary",
                "change",
                "decision",
                "tension",
                "risk",
                "question",
                "deduction",
            ],
            "allowed_hook_types": [
                "missing_context",
                "changed",
                "tension",
                "buried_decision",
                "person_involved",
                "official_vs_actual",
                "affects_work",
                "novel_synthesis",
                "none",
            ],
            "guidance": [
                "Only give high hook_strength if a busy teammate would feel compelled to click.",
                "Prefer evidence-backed deductions over generic summaries.",
                "Use the previous version only to identify meaningful changes or drift.",
                "A good claim implies: I missed something, this changed while I was gone, this rationale is buried, or this will affect my work soon.",
                "If a sentence is only a heading, title, metadata field, or source citation, ignore it.",
                "Novel synthesis is especially valuable when it combines source facts into a useful deduction or runbook-style implication.",
            ],
            "current_doc": current_meta,
            "previous_doc": previous_meta,
        }
    )
    return system_prompt, user_prompt


def tile_prompts(claim: ClaimRecord) -> Tuple[str, str]:
    system_prompt = (
        "You generate Foundation homepage tiles. Return strict JSON with title, body, hook_type, "
        "why_click, and score. The tile must create click tension, not summarize the page."
    )
    user_prompt = serialize_json(
        {
            "task": "Generate one homepage tile from this claim.",
            "requirements": [
                "Title under 9 words and should feel like a situation, not a topic label.",
                "Body under 28 words.",
                "Use concrete nouns.",
                "Mention the consequence or unresolved implication.",
                "Do not restate the document title.",
                "Do not sound like release notes or a summary card.",
                "Optimize for a teammate thinking: wait, what happened here?",
            ],
            "claim": asdict(claim),
        }
    )
    return system_prompt, user_prompt


def group_prompts(tiles: Sequence[CandidateTileRecord]) -> Tuple[str, str]:
    system_prompt = (
        "You write Foundation Currents. Ask yourself: what would make a busy teammate feel 'wait, I should open this'?"
        " Return strict JSON with title, body, narrative_type, why_this_group_matters, evidence, salience_score, and hook_quality_score. "
        "This is not a topic cluster. It is an editor-written storyline surfaced from the wiki."
    )
    user_prompt = serialize_json(
        {
            "task": "Write a Current from these candidate tiles.",
            "requirements": [
                "Title should be a journalistic hook or situation, not a topic.",
                "Title under 7 words.",
                "Body under 24 words.",
                "why_this_group_matters under 24 words.",
                "Emphasize missing context, divergence, buried rationale, unresolved tension, or near-term work impact.",
                "Narrative type must be one of: missing_context, change, unresolved_tension, buried_rationale, person_thread, official_vs_actual, near_term_impact, emerging_storyline.",
                "Evidence should be 2-4 short bullet-like strings explaining why these pages belong together.",
                "Salience score and hook quality score must be floats from 0 to 1.",
            ],
            "tiles": [asdict(tile) for tile in tiles],
        }
    )
    return system_prompt, user_prompt


def extract_claims_with_llm(
    llm: OpenAIJsonClient,
    current_doc: WikiDoc,
    previous_doc: Optional[WikiDoc],
    now: datetime,
) -> List[ClaimRecord]:
    system_prompt, user_prompt = extraction_prompts(current_doc, previous_doc)
    payload = llm.json_completion(system_prompt, user_prompt)
    created_at = now.isoformat()
    claims: List[ClaimRecord] = []
    for item in payload.get("claims", [])[:8]:
        claim_text = str(item.get("claim_text", "")).strip()
        if not claim_text:
            continue
        claims.append(
            ClaimRecord(
                id=f"claim_{current_doc.id}_{len(claims)}",
                wiki_doc_id=current_doc.id,
                slug=current_doc.slug,
                title=current_doc.title,
                claim_text=claim_text[:500],
                claim_type=coerce_claim_type(item.get("claim_type")),
                hook_type=coerce_hook_type(item.get("hook_type")),
                hook_strength=coerce_score(item.get("hook_strength"), 0.0),
                deduction_score=coerce_score(item.get("deduction_score"), 0.0),
                confidence=coerce_score(item.get("confidence"), 0.0),
                evidence=coerce_limited_strings(item.get("evidence"), 5),
                source_ids=current_doc.source_ids,
                entities=coerce_limited_strings(item.get("entities"), 8),
                people=coerce_limited_strings(item.get("people"), 5),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return claims


def tile_with_llm(
    llm: OpenAIJsonClient,
    claim: ClaimRecord,
    updated_at: Optional[str],
    now: datetime,
) -> CandidateTileRecord:
    fallback = fallback_tile_for_claim(claim, updated_at=updated_at, now=now)
    system_prompt, user_prompt = tile_prompts(claim)
    payload = llm.json_completion(system_prompt, user_prompt)
    fallback.title = str(payload.get("title", fallback.title))[:72] or fallback.title
    fallback.body = str(payload.get("body", fallback.body))[:180] or fallback.body
    fallback.why_click = str(payload.get("why_click", fallback.why_click))[:140] or fallback.why_click
    fallback.hook_type = coerce_hook_type(payload.get("hook_type"), default=fallback.hook_type)
    fallback.score = coerce_score(payload.get("score"), fallback.score)
    return fallback


def enrich_group_with_llm(
    llm: OpenAIJsonClient,
    group: TileGroupRecord,
    tiles: Sequence[CandidateTileRecord],
) -> TileGroupRecord:
    system_prompt, user_prompt = group_prompts(tiles)
    payload = llm.json_completion(system_prompt, user_prompt)
    group.title = str(payload.get("title", group.title))[:80] or group.title
    group.body = str(payload.get("body", group.body))[:160] or group.body
    narrative_type = str(payload.get("narrative_type", group.narrative_type)).strip()
    if narrative_type:
        group.narrative_type = narrative_type
    group.why_this_group_matters = (
        str(payload.get("why_this_group_matters", group.why_this_group_matters))[:120]
        or group.why_this_group_matters
    )
    group.evidence = coerce_limited_strings(payload.get("evidence"), 4) or group.evidence
    group.salience_score = coerce_score(payload.get("salience_score"), group.salience_score)
    group.hook_quality_score = coerce_score(payload.get("hook_quality_score"), group.hook_quality_score)
    group.score = coerce_score(
        payload.get("score"),
        round(clamp(0.6 * group.salience_score + 0.4 * group.hook_quality_score), 4),
    )
    return group


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate wiki homepage tilegroups.")
    parser.add_argument("--cloud-host", default="api.devchroma.com")
    parser.add_argument("--api-key")
    parser.add_argument("--tenant")
    parser.add_argument("--database")
    parser.add_argument("--collection", default="wiki")
    parser.add_argument("--revisions-collection", default="wiki_revisions")
    parser.add_argument("--selection-mode", choices=["global", "recent"], default="global")
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--scan-limit", type=int, default=5000)
    parser.add_argument("--output-dir", default="tmp/wiki_tilegroups")
    parser.add_argument("--openai-api-key")
    parser.add_argument("--openai-model", default="gpt-4.1-mini")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--print-json", action="store_true")
    return parser


def require_arg(value: Optional[str], env_name: str) -> str:
    if value:
        return value
    value = os.environ.get(env_name)
    if value:
        return value
    raise SystemExit(f"Missing required setting: {env_name}")


def persist_outputs(
    output_dir: Path,
    selected_docs: Sequence[Tuple[WikiDoc, Optional[WikiDoc]]],
    kept_claims: Sequence[ClaimRecord],
    rejected_claims: Sequence[RejectedClaimRecord],
    tiles: Sequence[CandidateTileRecord],
    groups: Sequence[Tuple[TileGroupRecord, List[CandidateTileRecord]]],
    debug: Dict[str, Any],
) -> Dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_jsonl(output_dir / "claims.jsonl", kept_claims)
    write_jsonl(output_dir / "rejected_claims.jsonl", rejected_claims)
    write_jsonl(output_dir / "candidate_tiles.jsonl", tiles)
    write_json(
        output_dir / "selected_docs.json",
        [
            {
                "current": asdict(current_doc),
                "previous": asdict(previous_doc) if previous_doc else None,
            }
            for current_doc, previous_doc in selected_docs
        ],
    )
    write_json(
        output_dir / "tilegroups.json",
        [
            {
                **asdict(group),
                "tiles": [asdict(tile) for tile in group_tiles],
            }
            for group, group_tiles in groups
        ],
    )
    write_json(
        output_dir / "currents.json",
        [
            {
                **asdict(group),
                "tiles": [asdict(tile) for tile in group_tiles],
            }
            for group, group_tiles in groups
        ],
    )
    summary = summarize_output(groups, debug)
    write_json(output_dir / "summary.json", summary)
    return summary


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    now = utc_now()

    api_key = require_arg(args.api_key, "CHROMA_API_KEY")
    tenant = require_arg(args.tenant, "CHROMA_TENANT")
    database = require_arg(args.database, "CHROMA_DATABASE")

    llm: Optional[OpenAIJsonClient] = None
    if not args.skip_llm:
        llm_api_key = args.openai_api_key or os.environ.get("OPENAI_API_KEY")
        if llm_api_key:
            llm = OpenAIJsonClient(api_key=llm_api_key, model=args.openai_model)
        else:
            args.skip_llm = True

    wiki_client = ChromaCloudWikiClient(
        cloud_host=args.cloud_host,
        api_key=api_key,
        tenant=tenant,
        database=database,
    )

    try:
        wiki_docs = wiki_client.fetch_wiki_docs(
            args.collection,
            page_size=args.page_size,
            scan_limit=args.scan_limit,
        )
        revision_docs = wiki_client.fetch_wiki_docs(
            args.revisions_collection,
            page_size=args.page_size,
            scan_limit=args.scan_limit,
        )
    finally:
        wiki_client.close()

    selected_docs = select_docs_for_processing(
        wiki_docs,
        revision_docs=revision_docs,
        mode=args.selection_mode,
        window_days=args.window_days,
        recent_limit=args.limit,
        now=now,
    )

    extracted_claims: List[ClaimRecord] = []
    kept_claims: List[ClaimRecord] = []
    rejected_claims: List[RejectedClaimRecord] = []
    tiles: List[CandidateTileRecord] = []

    for current_doc, previous_doc in selected_docs:
        if llm is not None and not args.skip_llm:
            claims = extract_claims_with_llm(llm, current_doc, previous_doc, now)
        else:
            claims = fallback_claims_for_doc(current_doc, previous_doc, now=now)
        extracted_claims.extend(claims)
        for claim in claims:
            keep, reason = should_keep_claim(claim)
            if keep:
                kept_claims.append(claim)
                if llm is not None and not args.skip_llm:
                    tile = tile_with_llm(llm, claim, current_doc.updated_at, now)
                else:
                    tile = fallback_tile_for_claim(claim, current_doc.updated_at, now=now)
                tiles.append(tile)
            else:
                rejected_claims.append(
                    RejectedClaimRecord(
                        claim_text=claim.claim_text,
                        hook_type=claim.hook_type,
                        hook_strength=claim.hook_strength,
                        deduction_score=claim.deduction_score,
                        confidence=claim.confidence,
                        rejection_reason=reason or "filtered_out",
                        wiki_doc_id=claim.wiki_doc_id,
                        slug=claim.slug,
                    )
                )

    groups = build_tilegroups(tiles, now=now)
    if llm is not None and not args.skip_llm:
        enriched: List[Tuple[TileGroupRecord, List[CandidateTileRecord]]] = []
        for group, group_tiles in groups:
            enriched.append((enrich_group_with_llm(llm, group, group_tiles), group_tiles))
        groups = enriched
        llm.close()

    debug = {
        "docs_scanned": len(wiki_docs),
        "revision_docs_scanned": len(revision_docs),
        "docs_processed": len(selected_docs),
        "claims_extracted": len(extracted_claims),
        "claims_kept": len(kept_claims),
        "claims_rejected": len(rejected_claims),
        "tiles_generated": len(tiles),
        "groups_generated": len(groups),
        "window_days": args.window_days,
        "selection_mode": args.selection_mode,
        "recent_limit": args.limit,
        "generated_at": now.isoformat(),
        "llm_used": llm is not None and not args.skip_llm,
    }

    summary = persist_outputs(
        Path(args.output_dir),
        selected_docs,
        kept_claims,
        rejected_claims,
        tiles,
        groups,
        debug,
    )
    if args.print_json:
        sys.stdout.write(json.dumps(summary, indent=2))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
