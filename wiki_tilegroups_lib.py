from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from uuid import NAMESPACE_URL, uuid5


CLAIM_TYPES = {
    "summary",
    "change",
    "decision",
    "tension",
    "risk",
    "question",
    "deduction",
}

HOOK_TYPES = {
    "missing_context",
    "changed",
    "tension",
    "buried_decision",
    "person_involved",
    "official_vs_actual",
    "affects_work",
    "novel_synthesis",
    "none",
}

NARRATIVE_TYPES = {
    "missing_context",
    "change",
    "unresolved_tension",
    "buried_rationale",
    "person_thread",
    "official_vs_actual",
    "near_term_impact",
    "emerging_storyline",
}

GROUP_WORTHY_HOOKS = {
    "missing_context",
    "tension",
    "buried_decision",
    "person_involved",
    "official_vs_actual",
    "affects_work",
    "novel_synthesis",
}
FALLBACK_TITLE_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "into",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "with",
}

NON_PERSON_NAME_TOKENS = {
    "Active",
    "Analysis",
    "Architecture",
    "Benchmarking",
    "Chroma",
    "Code",
    "Concepts",
    "Context",
    "Date",
    "Developers",
    "Duration",
    "Engineering",
    "Format",
    "Generative",
    "How",
    "Indexing",
    "Input",
    "Interoperability",
    "Introduction",
    "Kelly",  # allow via explicit person context below
    "Last",
    "Lazy",
    "LinkedIn",
    "Loading",
    "Models",
    "Most",
    "Notion",
    "Owner",
    "Part",
    "Performance",
    "Posting",
    "Proposal",
    "Queries",
    "Qdrant",
    "Research",
    "Retrievals",
    "See",
    "Shard",
    "Source",
    "Speaker",
    "Status",
    "Sales",
    "Telemetry",
    "Technical",
    "Title",
    "Understanding",
    "Updated",
    "Vector",
    "Video",
    "YouTube",
}

PERSON_FIELD_LABELS = {
    "owner",
    "owners",
    "reporter",
    "speaker",
    "author",
    "authors",
    "from",
}

METADATA_FIELD_LABELS = {
    "date",
    "updated",
    "owner",
    "owners",
    "status",
    "credits",
    "sources",
    "reporter",
    "speaker",
    "context",
    "related",
    "slack",
}

EDITORIAL_VERB_HINTS = {
    "added",
    "affect",
    "asked",
    "confused",
    "degrades",
    "drives",
    "failed",
    "hurting",
    "includes",
    "leaning",
    "misread",
    "points",
    "resolved",
    "shifted",
    "switched",
    "targets",
}

PERSON_THREAD_DEDUCTION_PATTERNS: List[Tuple[re.Pattern[str], str]] = [
    (re.compile(r"resolved by adding credits", re.IGNORECASE), "Startup credits are being patched manually"),
    (re.compile(r"high-signal leads?", re.IGNORECASE), "Sales is prioritizing a handful of high-signal leads"),
    (re.compile(r"separates thinking from doing", re.IGNORECASE), "Foundation's planning/doing split changes how work gets executed"),
    (re.compile(r"automated lead scoring", re.IGNORECASE), "Sales is using automated scoring to narrow outbound focus"),
]

OFFICIAL_VS_ACTUAL_PATTERNS: List[Tuple[re.Pattern[str], str, str]] = [
    (
        re.compile(r"not timeline-driven", re.IGNORECASE),
        "Onboarding expects trust to replace timelines",
        "The onboarding docs shift progress from deadlines to trust and agency, which changes how people are expected to orient to work.",
    ),
    (
        re.compile(r"expected to take significant time", re.IGNORECASE),
        "Onboarding is framed as principle-building, not speed",
        "The official process emphasizes long-term stewardship over fast completion, which may not match how new teammates expect onboarding to work.",
    ),
]

GLOBAL_SIGNAL_PHRASES = {
    "missing_context": [
        "see also",
        "for additional details",
        "follow-up",
        "not specified",
        "full details",
        "authoritative",
    ],
    "change": [
        "status:",
        "last updated",
        "changed",
        "new",
        "now",
        "announced",
        "launch",
        "in progress",
        "completed",
    ],
    "tension": [
        "however",
        "despite",
        "but",
        "instead",
        "skeptical",
        "unreliable",
        "degrades",
        "tradeoff",
    ],
    "buried_rationale": [
        "rationale",
        "motivation",
        "because",
        "decision",
        "chose",
        "chosen",
        "why",
    ],
    "official_vs_actual": [
        "official",
        "actual",
        "expected",
        "vs",
        "reality",
        "ceiling",
        "fundamental",
    ],
    "near_term_impact": [
        "will affect",
        "need",
        "blocking",
        "deadline",
        "follow-up report",
        "bug",
        "known issue",
        "critical",
        "support model",
    ],
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if not text:
        return None
    if re.fullmatch(r"\d+(?:\.\d+)?", text):
        try:
            return datetime.fromtimestamp(float(text), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def normalize_string_list(value: Any) -> List[str]:
    result: List[str] = []
    for item in ensure_list(value):
        if item is None:
            continue
        text = str(item).strip()
        if text:
            result.append(text)
    return result


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def jaccard(left: Iterable[str], right: Iterable[str]) -> float:
    left_set = {item for item in left if item}
    right_set = {item for item in right if item}
    if not left_set and not right_set:
        return 0.0
    intersection = left_set & right_set
    union = left_set | right_set
    return len(intersection) / len(union)


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def stable_id(prefix: str, parts: Sequence[Any]) -> str:
    payload = "||".join("" if part is None else str(part) for part in parts)
    return f"{prefix}_{uuid5(NAMESPACE_URL, payload)}"


@dataclass
class WikiDoc:
    id: str
    document: str
    categories: List[str]
    chunk_id: Optional[str]
    created_at: Optional[str]
    kind: Optional[str]
    line_no: Optional[int]
    slug: str
    source_ids: List[str]
    title: str
    updated_at: Optional[str]
    version: Optional[str]

    @property
    def updated_dt(self) -> Optional[datetime]:
        return parse_datetime(self.updated_at)

    @property
    def created_dt(self) -> Optional[datetime]:
        return parse_datetime(self.created_at)

    @classmethod
    def from_record(
        cls, record_id: str, document: Optional[str], metadata: Optional[Dict[str, Any]]
    ) -> "WikiDoc":
        metadata = metadata or {}
        line_no_value = metadata.get("line_no")
        try:
            line_no = int(line_no_value) if line_no_value is not None else None
        except (TypeError, ValueError):
            line_no = None
        return cls(
            id=str(record_id),
            document=document or "",
            categories=normalize_string_list(metadata.get("categories")),
            chunk_id=metadata.get("chunk_id"),
            created_at=metadata.get("created_at"),
            kind=metadata.get("kind"),
            line_no=line_no,
            slug=str(metadata.get("slug") or record_id),
            source_ids=normalize_string_list(metadata.get("source_ids")),
            title=str(metadata.get("title") or metadata.get("slug") or record_id),
            updated_at=metadata.get("updated_at"),
            version=str(metadata["version"]) if metadata.get("version") is not None else None,
        )


@dataclass
class ClaimRecord:
    id: str
    wiki_doc_id: str
    slug: str
    title: str
    claim_text: str
    claim_type: str
    hook_type: str
    hook_strength: float
    deduction_score: float
    confidence: float
    evidence: List[str]
    source_ids: List[str]
    entities: List[str]
    people: List[str]
    created_at: str
    updated_at: str


@dataclass
class RejectedClaimRecord:
    claim_text: str
    hook_type: str
    hook_strength: float
    deduction_score: float
    confidence: float
    rejection_reason: str
    wiki_doc_id: str
    slug: str


@dataclass
class CandidateTileRecord:
    id: str
    claim_ids: List[str]
    title: str
    body: str
    hook_type: str
    hook_strength: float
    deduction_score: float
    score: float
    why_click: str
    source_ids: List[str]
    slugs: List[str]
    entry_slug: str
    entry_title: str
    entry_path: str
    entities: List[str]
    created_at: str


@dataclass
class TileGroupRecord:
    id: str
    title: str
    body: str
    tile_ids: List[str]
    narrative_type: str
    score: float
    salience_score: float
    hook_quality_score: float
    slugs: List[str]
    entities: List[str]
    source_ids: List[str]
    page_ids: List[str]
    evidence: List[str]
    created_at: str
    updated_at: str
    why_this_group_matters: str


def freshness_score(updated_at: Optional[str], now: Optional[datetime] = None) -> float:
    now = now or utc_now()
    updated_dt = parse_datetime(updated_at)
    if updated_dt is None:
        return 0.3
    age = max(now - updated_dt, timedelta())
    if age <= timedelta(days=1):
        return 1.0
    if age >= timedelta(days=7):
        return 0.3
    age_days = age.total_seconds() / 86400.0
    # Linear decay from 1.0 at 1 day to 0.3 at 7 days.
    return clamp(1.0 - ((age_days - 1.0) / 6.0) * 0.7)


def source_diversity_score(source_ids: Sequence[str]) -> float:
    return clamp(len(set(source_ids)) / 4.0)


def document_change_score(
    current_doc: WikiDoc,
    previous_doc: Optional[WikiDoc],
) -> float:
    if previous_doc is None:
        return 0.0

    current_sentences = split_sentences(current_doc.document)
    previous_sentences = split_sentences(previous_doc.document)
    if not current_sentences:
        return 0.0

    previous_sentence_set = {sentence.strip() for sentence in previous_sentences if sentence.strip()}
    novel_sentences = [
        sentence for sentence in current_sentences if sentence.strip() not in previous_sentence_set
    ]
    sentence_delta = clamp(len(novel_sentences) / max(len(current_sentences), 1))

    lexical_drift = 1.0 - jaccard(tokenize(current_doc.document), tokenize(previous_doc.document))

    current_version = doc_version_number(current_doc) or 0
    previous_version = doc_version_number(previous_doc) or 0
    version_delta = clamp(max(current_version - previous_version, 0) / 3.0)

    return round(
        clamp(0.55 * sentence_delta + 0.30 * lexical_drift + 0.15 * version_delta),
        4,
    )


def document_signal_score(
    doc: WikiDoc,
    now: Optional[datetime] = None,
    revision_history: Optional[Sequence[WikiDoc]] = None,
) -> float:
    now = now or utc_now()
    lowered = doc.document.lower()
    freshness = freshness_score(doc.updated_at, now=now)
    source_diversity = source_diversity_score(doc.source_ids)
    richness = clamp(len(split_sentences(doc.document)) / 8.0)
    people_signal = 1.0 if has_person_signal(doc.document) else 0.0
    revision_count_signal = 0.0
    change_signal = 0.0
    if revision_history:
        revision_versions = {doc_version_number(revision) for revision in revision_history}
        revision_versions.discard(None)
        revision_count_signal = clamp(len(revision_versions) / 3.0)
        change_signal = document_change_score(doc, previous_revision_for_doc(doc, revision_history))

    narrative_hits = 0
    for phrases in GLOBAL_SIGNAL_PHRASES.values():
        if any(phrase in lowered for phrase in phrases):
            narrative_hits += 1
    narrative_density = clamp(narrative_hits / 5.0)

    return round(
        clamp(
            0.22 * freshness
            + 0.19 * narrative_density
            + 0.16 * richness
            + 0.15 * people_signal
            + 0.13 * source_diversity
            + 0.07 * revision_count_signal
            + 0.08 * change_signal
        ),
        4,
    )


def should_keep_claim(claim: ClaimRecord) -> Tuple[bool, Optional[str]]:
    if claim.hook_type not in HOOK_TYPES:
        return False, "invalid_hook_type"
    if claim.claim_type not in CLAIM_TYPES:
        return False, "invalid_claim_type"
    if (
        claim.hook_type != "none"
        and claim.hook_strength >= 0.6
        and claim.confidence >= 0.6
    ):
        return True, None
    if (
        claim.claim_type in {"tension", "decision", "deduction", "risk"}
        and claim.deduction_score >= 0.65
    ):
        return True, None
    if claim.hook_type == "none":
        return False, "hook_type_none"
    if claim.hook_strength < 0.6:
        return False, "hook_strength_below_threshold"
    if claim.confidence < 0.6:
        return False, "confidence_below_threshold"
    return False, "filtered_out"


def compute_tile_score(
    claim: ClaimRecord,
    updated_at: Optional[str],
    now: Optional[datetime] = None,
) -> float:
    fresh = freshness_score(updated_at, now=now)
    diversity = source_diversity_score(claim.source_ids)
    return round(
        clamp(
            0.40 * claim.hook_strength
            + 0.30 * claim.deduction_score
            + 0.20 * fresh
            + 0.10 * diversity
        ),
        4,
    )


def hook_compatibility(left: str, right: str) -> float:
    if left == right:
        return 1.0
    pair = {left, right}
    if pair == {"tension", "official_vs_actual"}:
        return 1.0
    if pair == {"buried_decision", "changed"}:
        return 0.7
    if "affects_work" in pair:
        return 0.5
    return 0.0


def tile_similarity(left: CandidateTileRecord, right: CandidateTileRecord) -> float:
    return round(
        0.35 * jaccard(left.entities, right.entities)
        + 0.30 * jaccard(left.slugs, right.slugs)
        + 0.25 * jaccard(left.source_ids, right.source_ids)
        + 0.10 * hook_compatibility(left.hook_type, right.hook_type),
        4,
    )


def average_pairwise_similarity(tiles: Sequence[CandidateTileRecord]) -> float:
    if len(tiles) < 2:
        return 0.0
    similarities: List[float] = []
    for index, left in enumerate(tiles):
        for right in tiles[index + 1 :]:
            similarities.append(tile_similarity(left, right))
    if not similarities:
        return 0.0
    return round(sum(similarities) / len(similarities), 4)


def deduction_density(tiles: Sequence[CandidateTileRecord]) -> float:
    if not tiles:
        return 0.0
    matching = [tile for tile in tiles if tile.deduction_score >= 0.65]
    return round(len(matching) / len(tiles), 4)


def infer_narrative_type(hook_type: str) -> str:
    mapping = {
        "missing_context": "missing_context",
        "changed": "change",
        "tension": "unresolved_tension",
        "buried_decision": "buried_rationale",
        "person_involved": "person_thread",
        "official_vs_actual": "official_vs_actual",
        "affects_work": "near_term_impact",
        "novel_synthesis": "emerging_storyline",
    }
    return mapping.get(hook_type, "emerging_storyline")


def split_sentences(document: str) -> List[str]:
    if not document.strip():
        return []
    cleaned_lines: List[str] = []
    for raw_line in document.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            continue
        if line.startswith("**Source:**"):
            continue
        if line.startswith("**Status:**"):
            continue
        if re.fullmatch(r"[-*]\s+.+", line):
            cleaned_lines.append(line[2:].strip())
            continue
        cleaned_lines.append(line)
    normalized = re.sub(r"\s+", " ", " ".join(cleaned_lines))
    candidates = re.split(r"(?<=[.!?])\s+", normalized)
    results: List[str] = []
    for sentence in candidates:
        cleaned = sentence.strip(" -\n\t")
        if len(cleaned) >= 24:
            results.append(cleaned)
    return results


def guess_hook_type(text: str, changed: bool = False) -> str:
    lowered = text.lower()
    if any(
        phrase in lowered
        for phrase in [
            "see also",
            "for additional details",
            "not specified",
            "follow-up",
            "deeper dive",
        ]
    ):
        return "missing_context"
    if has_person_signal(text):
        return "person_involved"
    if changed or any(word in lowered for word in ["changed", "new", "now", "added"]):
        return "changed"
    if any(
        phrase in lowered
        for phrase in [
            "this research directly addresses",
            "this implies",
            "this suggests",
            "this means",
            "informs better",
            "golden datasets are critical",
        ]
    ):
        return "novel_synthesis"
    if any(word in lowered for word in ["risk", "blocked", "failure", "outage", "break"]):
        return "affects_work"
    if any(word in lowered for word in ["but", "however", "despite", "instead"]):
        return "tension"
    if any(word in lowered for word in ["decision", "decided", "direction"]):
        return "buried_decision"
    if any(word in lowered for word in ["actual", "official", "expected"]):
        return "official_vs_actual"
    return "none"


def guess_claim_type(text: str, changed: bool = False) -> str:
    lowered = text.lower()
    if changed:
        return "change"
    if "?" in text or lowered.startswith("why ") or lowered.startswith("how "):
        return "question"
    if any(word in lowered for word in ["risk", "blocked", "failure", "issue"]):
        return "risk"
    if any(word in lowered for word in ["decision", "decided", "direction"]):
        return "decision"
    if any(word in lowered for word in ["but", "however", "despite", "instead"]):
        return "tension"
    if any(word in lowered for word in ["means", "implies", "therefore", "suggests"]):
        return "deduction"
    if has_person_signal(text):
        return "decision"
    return "summary"


def extract_entities(*parts: str) -> List[str]:
    text = " ".join(parts)
    found = re.findall(r"\b[A-Z][a-zA-Z0-9_-]{2,}\b", text)
    unique: List[str] = []
    seen = set()
    for item in found:
        if item.lower() in FALLBACK_TITLE_STOPWORDS:
            continue
        if item not in seen:
            seen.add(item)
            unique.append(item)
    return unique[:8]


def extract_people(*parts: str) -> List[str]:
    text = " ".join(parts)
    candidates = re.findall(r"\b[A-Z][a-z]+ [A-Z][a-z]+\b", text)
    unique: List[str] = []
    seen = set()
    for candidate in candidates:
        first, last = candidate.split()
        if first in NON_PERSON_NAME_TOKENS or last in NON_PERSON_NAME_TOKENS:
            continue
        if candidate not in seen:
            seen.add(candidate)
            unique.append(candidate)
    return unique[:5]


def has_person_signal(text: str) -> bool:
    lowered = text.lower()
    if any(
        marker in lowered
        for marker in [
            "owner:",
            "speaker:",
            "cofounder",
            "founder",
            "hammad",
            "jeff",
        ]
    ):
        return True
    return bool(extract_people(text))


def extract_named_people_for_current(tiles: Sequence[CandidateTileRecord]) -> List[str]:
    names: List[str] = []
    for tile in tiles:
        names.extend(extract_people(tile.title, tile.body))
        text = strip_markup(f"{tile.title} {tile.body}")
        for label in PERSON_FIELD_LABELS:
            pattern = rf"\b{label}s?:\s*([^.;|\n]+)"
            for match in re.findall(pattern, text, flags=re.IGNORECASE):
                for paren_match in re.findall(r"\(([^)]+)\)", match):
                    for token in re.split(r"[,/]| and ", paren_match):
                        token = strip_markup(token)
                        if re.fullmatch(r"[A-Z][a-z]+(?: [A-Z][a-z]+)?", token) and token not in NON_PERSON_NAME_TOKENS:
                            names.append(token)
                match = re.sub(r"\([^)]*\)", "", match).strip()
                for token in re.split(r"[,/]| and ", match):
                    token = strip_markup(token)
                    if re.fullmatch(r"[A-Z][a-z]+(?: [A-Z][a-z]+)?", token) and token not in NON_PERSON_NAME_TOKENS:
                        names.append(token)
    return list(dict.fromkeys(names))


def fallback_claims_for_doc(
    current_doc: WikiDoc,
    previous_doc: Optional[WikiDoc],
    now: Optional[datetime] = None,
) -> List[ClaimRecord]:
    now = now or utc_now()
    current_sentences = split_sentences(current_doc.document)
    previous_sentences = set(split_sentences(previous_doc.document)) if previous_doc else set()
    created_at = now.isoformat()
    claims: List[ClaimRecord] = []
    for sentence in current_sentences[:8]:
        changed = previous_doc is not None and sentence not in previous_sentences
        claim_type = guess_claim_type(sentence, changed=changed)
        hook_type = guess_hook_type(sentence, changed=changed)
        hook_strength = 0.82 if hook_type in GROUP_WORTHY_HOOKS else 0.4
        deduction_score = (
            0.82 if hook_type in {"official_vs_actual", "novel_synthesis", "missing_context"} else
            0.72 if claim_type in {"deduction", "tension", "risk", "decision"} else
            0.35
        )
        confidence = 0.7 if hook_type != "none" else 0.5
        claims.append(
            ClaimRecord(
                id=stable_id("claim", [current_doc.id, sentence]),
                wiki_doc_id=current_doc.id,
                slug=current_doc.slug,
                title=current_doc.title,
                claim_text=sentence[:400],
                claim_type=claim_type,
                hook_type=hook_type,
                hook_strength=hook_strength,
                deduction_score=deduction_score,
                confidence=confidence,
                evidence=[sentence[:220]],
                source_ids=current_doc.source_ids,
                entities=extract_entities(current_doc.title, sentence),
                people=extract_people(sentence),
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return claims[:8]


def fallback_tile_for_claim(
    claim: ClaimRecord, updated_at: Optional[str], now: Optional[datetime] = None
) -> CandidateTileRecord:
    now = now or utc_now()
    words = claim.claim_text.split()
    raw_title = " ".join(words[:8]).strip(" ,.;:")
    if len(raw_title.split()) < 3:
        raw_title = claim.title
    body = claim.claim_text[:140].strip()
    why_click = body[:120]
    score = compute_tile_score(claim, updated_at=updated_at, now=now)
    created_at = now.isoformat()
    return CandidateTileRecord(
        id=stable_id("tile", [claim.id, raw_title]),
        claim_ids=[claim.id],
        title=raw_title[:72],
        body=body[:180],
        hook_type=claim.hook_type,
        hook_strength=claim.hook_strength,
        deduction_score=claim.deduction_score,
        score=score,
        why_click=why_click,
        source_ids=claim.source_ids,
        slugs=[claim.slug],
        entry_slug=claim.slug,
        entry_title=claim.title[:120],
        entry_path=f"/{claim.slug}",
        entities=claim.entities,
        created_at=created_at,
    )


def fallback_group_copy(
    tiles: Sequence[CandidateTileRecord],
) -> Tuple[str, str, str, str]:
    hook_counts = defaultdict(int)
    for tile in tiles:
        hook_counts[tile.hook_type] += 1
    primary_hook = max(hook_counts.items(), key=lambda item: item[1])[0]
    keywords: List[str] = []
    for tile in tiles:
        for token in tokenize(tile.title):
            if token not in FALLBACK_TITLE_STOPWORDS and token not in keywords:
                keywords.append(token)
    title = " ".join(word.capitalize() for word in keywords[:6]) or "Work Is Shifting"
    body = tiles[0].body[:110] if tiles else "Several related signals are worth reviewing."
    why = tiles[0].why_click[:110] if tiles else body
    return title[:80], body[:160], primary_hook, why[:120]


def strip_markup(text: str) -> str:
    cleaned = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
    cleaned = re.sub(r"`([^`]+)`", r"\1", cleaned)
    cleaned = cleaned.replace("**", "").replace("__", "")
    cleaned = cleaned.replace("#", "").replace(">", "")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip(" -:;,.")


def remove_metadata_prefixes(text: str) -> str:
    cleaned = strip_markup(text)
    label_pattern = "|".join(sorted(METADATA_FIELD_LABELS, key=len, reverse=True))
    cleaned = re.sub(rf"\b(?:{label_pattern})s?:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"^(?:[A-Z][a-z]+ \d{4}\s+)+", "", cleaned)
    cleaned = re.sub(r"^(?:[A-Z][a-z]+(?: [A-Z][a-z]+)?) \([^)]*\)\s+", "", cleaned)
    while True:
        updated = re.sub(r"^[A-Z][a-z]+:\s*", "", cleaned).strip()
        if updated == cleaned:
            break
        cleaned = updated
    return cleaned.strip(" -:;,.")


def editorial_phrase(text: str, max_words: int = 10) -> str:
    cleaned = remove_metadata_prefixes(text)
    segments = re.split(r"[.;|]", cleaned)
    for segment in segments:
        segment = segment.strip(" -:;,.")
        if not segment:
            continue
        lowered = segment.lower()
        if any(f"{label}:" in lowered for label in METADATA_FIELD_LABELS):
            continue
        if any(hint in lowered for hint in EDITORIAL_VERB_HINTS):
            return shorten_phrase(segment, max_words)
    return shorten_phrase(cleaned or text, max_words)


def shorten_phrase(text: str, max_words: int = 10) -> str:
    words = strip_markup(text).split()
    return " ".join(words[:max_words]).strip(" -:;,.")


def editorial_tile_priority(tile: CandidateTileRecord) -> Tuple[float, int, int]:
    text = strip_markup(f"{tile.title} {tile.body}")
    lowered = text.lower()
    metadata_hits = sum(1 for label in METADATA_FIELD_LABELS if f"{label}:" in lowered)
    verb_bonus = 1 if any(hint in lowered for hint in EDITORIAL_VERB_HINTS) else 0
    return (tile.score - 0.03 * metadata_hits + 0.02 * verb_bonus, verb_bonus, -metadata_hits)


def best_tile(tiles: Sequence[CandidateTileRecord], hook_type: Optional[str] = None) -> Optional[CandidateTileRecord]:
    filtered = [tile for tile in tiles if hook_type is None or tile.hook_type == hook_type]
    if not filtered:
        return None
    return max(filtered, key=editorial_tile_priority)


def group_text_blob(tiles: Sequence[CandidateTileRecord]) -> str:
    return " ".join(strip_markup(f"{tile.title}. {tile.body}") for tile in tiles)


def deduction_tile(tiles: Sequence[CandidateTileRecord]) -> Optional[CandidateTileRecord]:
    if not tiles:
        return None
    return max(
        tiles,
        key=lambda tile: (
            tile.deduction_score,
            editorial_tile_priority(tile),
        ),
    )


def phrase_from_tile(tile: Optional[CandidateTileRecord], max_words: int = 9) -> str:
    if tile is None:
        return ""
    return editorial_phrase(tile.body or tile.title, max_words)


def tile_novelty_score(
    candidate: CandidateTileRecord,
    selected: Sequence[CandidateTileRecord],
) -> float:
    if not selected:
        return 1.0

    max_similarity = max(tile_similarity(candidate, tile) for tile in selected)
    new_slug_bonus = 0.12 if all(tile.entry_slug != candidate.entry_slug for tile in selected) else 0.0
    new_hook_bonus = 0.08 if all(tile.hook_type != candidate.hook_type for tile in selected) else 0.0
    return round(clamp((1.0 - max_similarity) + new_slug_bonus + new_hook_bonus), 4)


def rerank_tiles_for_current(
    ranked_tiles: Sequence[CandidateTileRecord],
    limit: int = 5,
) -> List[CandidateTileRecord]:
    remaining = list(ranked_tiles)
    selected: List[CandidateTileRecord] = []

    while remaining and len(selected) < limit:
        if not selected:
            next_tile = max(remaining, key=editorial_tile_priority)
        else:
            next_tile = max(
                remaining,
                key=lambda tile: (
                    round(0.72 * tile.score + 0.28 * tile_novelty_score(tile, selected), 4),
                    editorial_tile_priority(tile),
                ),
            )
        selected.append(next_tile)
        remaining.remove(next_tile)

    return selected


def select_tiles_for_current(
    ranked_tiles: Sequence[CandidateTileRecord],
    limit: int = 5,
) -> List[CandidateTileRecord]:
    primary = rerank_tiles_for_current(ranked_tiles, limit=limit)
    if len(primary) >= limit:
        return primary

    selected = list(primary)
    for tile in ranked_tiles:
        if tile in selected:
            continue
        selected.append(tile)
        if len(selected) >= limit:
            break
    return selected


def editorialize_current(
    title: str,
    body: str,
    narrative_type: str,
    why: str,
    tiles: Sequence[CandidateTileRecord],
) -> Tuple[str, str, str]:
    top = best_tile(tiles) or (tiles[0] if tiles else None)
    if top is None:
        return title, body, why

    raw = strip_markup(top.body or top.title)
    blob = group_text_blob(tiles)
    people = extract_named_people_for_current(tiles)
    deduction = deduction_tile(tiles)
    deduction_hook = phrase_from_tile(deduction, 10)

    if narrative_type == "missing_context":
        hook = deduction_hook or editorial_phrase(raw, 8)
        acronym = None
        for tile in tiles:
            text = strip_markup(f"{tile.title} {tile.body}")
            match = re.search(r"\b([A-Z]{2,5})\s*=", text)
            if match:
                acronym = match.group(1)
                break
        if acronym and "codex" in raw.lower():
            title = f"Codex took {acronym} to mean the wrong thing"
        else:
            title = f"You may be missing: {hook}".strip()
        body = "Foundation connected the acronym confusion to the missing context that caused the failure."
        why = "This is a deduction from the surrounding context, not just a quoted excerpt."
    elif narrative_type == "official_vs_actual":
        hook = deduction_hook or editorial_phrase(raw, 8)
        matched = False
        for pattern, inferred_title, inferred_body in OFFICIAL_VS_ACTUAL_PATTERNS:
            if pattern.search(blob):
                title = inferred_title
                body = inferred_body
                matched = True
                break
        if not matched:
            title = "The story and reality may differ"
            body = f"{hook} suggests the official framing does not fully match how the work actually operates."
        why = "Foundation is inferring a mismatch between the official process and how the work is actually meant to happen."
    elif narrative_type == "buried_rationale":
        hook = deduction_hook or editorial_phrase(raw, 8)
        title = "The rationale is buried"
        body = f"{hook} points to a decision, but not the reasoning teammates would need to trust it."
        why = "Foundation can see the decision trail, but the justification is still hard to recover."
    elif narrative_type == "unresolved_tension":
        hook = deduction_hook or editorial_phrase(raw, 9)
        title = "There is a live tension here"
        body = f"{hook} suggests the situation is not fully resolved yet."
        why = "Foundation sees a contradiction across the related pages, not just a single isolated statement."
    elif narrative_type == "near_term_impact":
        hook = deduction_hook or editorial_phrase(raw, 8)
        title = "This may affect work soon"
        body = f"{hook} looks like something teammates may need to react to soon."
        why = "The implication here is operational, not just informational."
    elif narrative_type == "person_thread":
        person = people[0] if people else None
        hook = deduction_hook or editorial_phrase(raw, 8)
        inferred = None
        for pattern, inferred_title in PERSON_THREAD_DEDUCTION_PATTERNS:
            if pattern.search(blob):
                inferred = inferred_title
                break
        if inferred:
            title = inferred
            if person:
                body = f"Foundation links this storyline to {person}, but the deduction is that {inferred.lower()}."
            else:
                body = f"Foundation links these pages into a single storyline: {inferred.lower()}."
            why = "This current is useful because it infers the operational takeaway behind the person thread."
        else:
            if person:
                title = f"What {person} is really driving"
                body = f"{hook} suggests {person} is central to a broader storyline, not just an isolated update."
            else:
                title = hook[:80] or "A person thread runs through this"
                body = f"{hook} suggests a person-led storyline with a broader implication."
            why = "The value here is the inferred storyline around the person, not the raw mention itself."
    else:
        hook = deduction_hook or editorial_phrase(raw, 8)
        title = hook[:80] or title
        body = f"{hook} points to a broader storyline worth understanding."
        why = "Several related pages add up to a broader storyline."

    title = strip_markup(title)[:80]
    body = strip_markup(body)[:160]
    why = strip_markup(why)[:120]
    return title, body, why


def build_tilegroups(
    tiles: Sequence[CandidateTileRecord],
    now: Optional[datetime] = None,
) -> List[Tuple[TileGroupRecord, List[CandidateTileRecord]]]:
    now = now or utc_now()
    adjacency: Dict[str, set[str]] = {tile.id: set() for tile in tiles}
    tiles_by_id = {tile.id: tile for tile in tiles}
    for index, left in enumerate(tiles):
        for right in tiles[index + 1 :]:
            similarity = tile_similarity(left, right)
            if similarity >= 0.35:
                adjacency[left.id].add(right.id)
                adjacency[right.id].add(left.id)

    components: List[List[CandidateTileRecord]] = []
    seen: set[str] = set()
    for tile in tiles:
        if tile.id in seen:
            continue
        queue = [tile.id]
        component_ids: List[str] = []
        while queue:
            current = queue.pop()
            if current in seen:
                continue
            seen.add(current)
            component_ids.append(current)
            queue.extend(adjacency[current] - seen)
        components.append([tiles_by_id[tile_id] for tile_id in component_ids])

    groups: List[Tuple[TileGroupRecord, List[CandidateTileRecord]]] = []
    for component in components:
        ranked_tiles = sorted(component, key=lambda tile: tile.score, reverse=True)
        if len(ranked_tiles) < 2:
            continue
        selected_tiles = select_tiles_for_current(ranked_tiles, limit=5)
        if not any(tile.hook_type in GROUP_WORTHY_HOOKS for tile in selected_tiles):
            continue
        coherence = average_pairwise_similarity(selected_tiles)
        density = deduction_density(selected_tiles)
        max_score = max(tile.score for tile in selected_tiles)
        avg_score = sum(tile.score for tile in selected_tiles) / len(selected_tiles)
        salience_score = round(
            clamp(0.45 * max_score + 0.25 * avg_score + 0.20 * coherence + 0.10 * density),
            4,
        )
        hook_quality_score = round(
            clamp(
                0.45 * max(tile.hook_strength for tile in selected_tiles)
                + 0.35 * coherence
                + 0.20 * density
            ),
            4,
        )
        group_score = round(clamp(0.6 * salience_score + 0.4 * hook_quality_score), 4)
        title, body, primary_hook, why = fallback_group_copy(selected_tiles)
        evidence = []
        for tile in selected_tiles:
            evidence.append(f"{tile.title} [{tile.hook_type}]")
        evidence = evidence[:6]
        narrative_type = infer_narrative_type(primary_hook)
        edited_title, edited_body, edited_why = editorialize_current(
            title,
            body,
            narrative_type,
            why,
            selected_tiles,
        )
        group = TileGroupRecord(
            id=stable_id("tilegroup", [edited_title, ",".join(tile.id for tile in selected_tiles)]),
            title=edited_title,
            body=edited_body,
            tile_ids=[tile.id for tile in selected_tiles],
            narrative_type=narrative_type,
            score=group_score,
            salience_score=salience_score,
            hook_quality_score=hook_quality_score,
            slugs=sorted({slug for tile in selected_tiles for slug in tile.slugs}),
            entities=sorted({entity for tile in selected_tiles for entity in tile.entities}),
            source_ids=sorted({source_id for tile in selected_tiles for source_id in tile.source_ids}),
            page_ids=sorted({claim_id for tile in selected_tiles for claim_id in tile.claim_ids}),
            evidence=evidence,
            created_at=now.isoformat(),
            updated_at=now.isoformat(),
            why_this_group_matters=edited_why,
        )
        groups.append((group, selected_tiles))

    groups.sort(key=lambda item: item[0].score, reverse=True)
    return groups[:5]


def latest_docs_by_slug(docs: Sequence[WikiDoc]) -> Dict[str, WikiDoc]:
    grouped: Dict[str, List[WikiDoc]] = defaultdict(list)
    for doc in canonicalize_docs(docs):
        grouped[doc.slug].append(doc)
    result: Dict[str, WikiDoc] = {}
    for slug, slug_docs in grouped.items():
        sorted_docs = sorted(
            slug_docs,
            key=lambda doc: (
                doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
                doc.version or "",
            ),
            reverse=True,
        )
        result[slug] = sorted_docs[0]
    return result


def previous_doc_for_slug(slug_docs: Sequence[WikiDoc], current_doc_id: str) -> Optional[WikiDoc]:
    slug_docs = canonicalize_docs(slug_docs)
    sorted_docs = sorted(
        slug_docs,
        key=lambda doc: (
            doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
            doc.version or "",
        ),
        reverse=True,
    )
    for index, doc in enumerate(sorted_docs):
        if doc.id == current_doc_id and index + 1 < len(sorted_docs):
            return sorted_docs[index + 1]
    return None


def doc_version_number(doc: WikiDoc) -> Optional[int]:
    if doc.version is None:
        return None
    try:
        return int(str(doc.version))
    except (TypeError, ValueError):
        return None


def previous_revision_for_doc(current_doc: WikiDoc, revision_docs: Sequence[WikiDoc]) -> Optional[WikiDoc]:
    canonical_revisions = canonicalize_docs(revision_docs)
    current_version = doc_version_number(current_doc)
    if current_version is None:
        return None
    older_revisions = [
        revision
        for revision in canonical_revisions
        if (doc_version_number(revision) or -1) < current_version
    ]
    if not older_revisions:
        return None
    older_revisions.sort(
        key=lambda doc: (
            doc_version_number(doc) or -1,
            doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
        ),
        reverse=True,
    )
    return older_revisions[0]


def version_key(doc: WikiDoc) -> Tuple[str, str]:
    updated = doc.updated_at or ""
    version = doc.version or ""
    if version:
        return version, updated
    return updated, doc.created_at or ""


def canonical_doc_for_version(docs: Sequence[WikiDoc]) -> WikiDoc:
    return max(
        docs,
        key=lambda doc: (
            len(doc.document or ""),
            len(doc.source_ids),
            0 if doc.line_no is None else doc.line_no,
        ),
    )


def canonicalize_docs(docs: Sequence[WikiDoc]) -> List[WikiDoc]:
    grouped: Dict[Tuple[str, Tuple[str, str]], List[WikiDoc]] = defaultdict(list)
    for doc in docs:
        grouped[(doc.slug, version_key(doc))].append(doc)
    canonical_docs = [canonical_doc_for_version(group) for group in grouped.values()]
    canonical_docs.sort(
        key=lambda doc: (
            doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
            doc.version or "",
        ),
        reverse=True,
    )
    return canonical_docs


def select_docs_for_processing(
    docs: Sequence[WikiDoc],
    revision_docs: Optional[Sequence[WikiDoc]] = None,
    mode: str = "global",
    window_days: int = 7,
    recent_limit: int = 50,
    now: Optional[datetime] = None,
) -> List[Tuple[WikiDoc, Optional[WikiDoc]]]:
    now = now or utc_now()
    docs = canonicalize_docs(docs)
    revision_docs = canonicalize_docs(revision_docs or [])
    docs_by_slug: Dict[str, List[WikiDoc]] = defaultdict(list)
    for doc in docs:
        docs_by_slug[doc.slug].append(doc)
    revisions_by_slug: Dict[str, List[WikiDoc]] = defaultdict(list)
    for doc in revision_docs:
        revisions_by_slug[doc.slug].append(doc)
    latest_by_slug = latest_docs_by_slug(docs)
    latest_docs = list(latest_by_slug.values())
    target_docs: List[WikiDoc]

    if mode == "recent":
        window_start = now - timedelta(days=window_days)
        recent_docs = [
            doc
            for doc in latest_docs
            if doc.updated_dt is not None and doc.updated_dt >= window_start
        ]
        recent_docs.sort(
            key=lambda doc: doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )
        target_docs = recent_docs[:recent_limit]
    else:
        target_docs = sorted(
            latest_docs,
            key=lambda doc: (
                document_signal_score(doc, now=now, revision_history=revisions_by_slug.get(doc.slug)),
                doc.updated_dt or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )[:recent_limit]

    selected: List[Tuple[WikiDoc, Optional[WikiDoc]]] = []
    for current_doc in target_docs:
        previous_doc = previous_revision_for_doc(current_doc, revisions_by_slug.get(current_doc.slug, []))
        if previous_doc is None:
            previous_doc = previous_doc_for_slug(docs_by_slug[current_doc.slug], current_doc.id)
        selected.append((current_doc, previous_doc))
    return selected


def serialize_json(data: Any) -> str:
    return json.dumps(data, indent=2, sort_keys=True)


def write_json(path: Path, data: Any) -> None:
    path.write_text(serialize_json(data) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: Sequence[Any]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            payload = asdict(row) if not isinstance(row, dict) else row
            handle.write(json.dumps(payload, sort_keys=True))
            handle.write("\n")


def summarize_output(
    groups: Sequence[Tuple[TileGroupRecord, List[CandidateTileRecord]]],
    debug: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "currents": [
            {
                "title": group.title,
                "body": group.body,
                "score": group.score,
                "narrative_type": group.narrative_type,
                "salience_score": group.salience_score,
                "hook_quality_score": group.hook_quality_score,
                "evidence": group.evidence,
                "tiles": [
                    {
                        "title": tile.title,
                        "body": tile.body,
                        "hook_type": tile.hook_type,
                        "score": tile.score,
                        "entry_slug": tile.entry_slug,
                        "entry_title": tile.entry_title,
                        "entry_path": tile.entry_path,
                        "source_ids": tile.source_ids,
                        "claim_ids": tile.claim_ids,
                    }
                    for tile in tiles
                ],
            }
            for group, tiles in groups
        ],
        "debug": debug,
    }


def content_hash(*parts: str) -> str:
    digest = hashlib.sha256("||".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]
