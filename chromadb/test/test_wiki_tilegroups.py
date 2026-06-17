from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

from wiki_tilegroups_lib import (
    CandidateTileRecord,
    ClaimRecord,
    TileGroupRecord,
    build_tilegroups,
    canonicalize_docs,
    compute_tile_score,
    document_signal_score,
    editorialize_current,
    freshness_score,
    select_docs_for_processing,
    select_tiles_for_current,
    should_keep_claim,
    summarize_output,
    WikiDoc,
)

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "bin" / "generate_wiki_tilegroups.py"
_SCRIPT_SPEC = importlib.util.spec_from_file_location("generate_wiki_tilegroups", _SCRIPT_PATH)
assert _SCRIPT_SPEC is not None and _SCRIPT_SPEC.loader is not None
generate_wiki_tilegroups = importlib.util.module_from_spec(_SCRIPT_SPEC)
_SCRIPT_SPEC.loader.exec_module(generate_wiki_tilegroups)


def _claim(**overrides) -> ClaimRecord:
    base = {
        "id": "claim-1",
        "wiki_doc_id": "doc-1",
        "slug": "sync-pipeline",
        "title": "Sync pipeline",
        "claim_text": "Connector auth can complete while async backfills are still running.",
        "claim_type": "deduction",
        "hook_type": "official_vs_actual",
        "hook_strength": 0.82,
        "deduction_score": 0.88,
        "confidence": 0.79,
        "evidence": ["Connector auth can complete first."],
        "source_ids": ["source-a", "source-b"],
        "entities": ["Connector", "Backfills"],
        "people": [],
        "created_at": "2026-06-06T00:00:00+00:00",
        "updated_at": "2026-06-06T00:00:00+00:00",
    }
    base.update(overrides)
    return ClaimRecord(**base)


def _tile(
    tile_id: str,
    *,
    slug: str,
    hook_type: str,
    entities: list[str],
    source_ids: list[str],
    score: float,
    deduction_score: float = 0.8,
) -> CandidateTileRecord:
    return CandidateTileRecord(
        id=tile_id,
        claim_ids=[f"{tile_id}-claim"],
        title=f"{slug} tile",
        body="A consequence-heavy tile body.",
        hook_type=hook_type,
        hook_strength=0.8,
        deduction_score=deduction_score,
        score=score,
        why_click="This matters soon.",
        source_ids=source_ids,
        slugs=[slug],
        entry_slug=slug,
        entry_title=f"{slug} page",
        entry_path=f"/{slug}",
        entities=entities,
        created_at="2026-06-06T00:00:00+00:00",
    )


def test_freshness_score_decays_between_day_one_and_day_seven() -> None:
    now = datetime(2026, 6, 6, tzinfo=timezone.utc)
    assert freshness_score((now - timedelta(hours=12)).isoformat(), now=now) == 1.0
    assert freshness_score((now - timedelta(days=8)).isoformat(), now=now) == 0.3
    mid = freshness_score((now - timedelta(days=4)).isoformat(), now=now)
    assert 0.3 < mid < 1.0


def test_should_keep_claim_accepts_hooky_claims() -> None:
    keep, reason = should_keep_claim(_claim())
    assert keep is True
    assert reason is None


def test_should_keep_claim_accepts_strong_deduction_even_if_hook_is_weak() -> None:
    keep, reason = should_keep_claim(
        _claim(hook_type="none", hook_strength=0.2, claim_type="deduction", deduction_score=0.9)
    )
    assert keep is True
    assert reason is None


def test_compute_tile_score_uses_weighted_formula() -> None:
    now = datetime(2026, 6, 6, tzinfo=timezone.utc)
    score = compute_tile_score(_claim(), updated_at=now.isoformat(), now=now)
    assert score > 0.8


def test_build_tilegroups_keeps_multi_tile_components_with_groupworthy_hooks() -> None:
    now = datetime(2026, 6, 6, tzinfo=timezone.utc)
    tiles = [
        _tile(
            "tile-1",
            slug="sync-pipeline",
            hook_type="official_vs_actual",
            entities=["Connector", "Backfills"],
            source_ids=["source-a", "source-b"],
            score=0.91,
        ),
        _tile(
            "tile-2",
            slug="sync-pipeline",
            hook_type="tension",
            entities=["Connector", "Trust"],
            source_ids=["source-b", "source-c"],
            score=0.86,
        ),
        _tile(
            "tile-3",
            slug="another-topic",
            hook_type="changed",
            entities=["Separate"],
            source_ids=["source-z"],
            score=0.55,
            deduction_score=0.2,
        ),
    ]
    groups = build_tilegroups(tiles, now=now)
    assert len(groups) == 1
    group, group_tiles = groups[0]
    assert isinstance(group, TileGroupRecord)
    assert len(group_tiles) == 2
    assert group.score > 0.7


def test_extract_claims_with_llm_sanitizes_invalid_enum_and_score_fields() -> None:
    class FakeLlm:
        def json_completion(self, system_prompt: str, user_prompt: str) -> dict:
            return {
                "claims": [
                    {
                        "claim_text": "Pipeline auth finished before the sync completed.",
                        "claim_type": "wild_guess",
                        "hook_type": "bad_hook",
                        "hook_strength": "1.7",
                        "deduction_score": "-2",
                        "confidence": "nan",
                        "evidence": [" A ", "", None, "B"],
                        "entities": [" Connector ", "", None],
                        "people": [" Jane Doe ", ""],
                    }
                ]
            }

    doc = generate_wiki_tilegroups.WikiDoc(
        id="doc-1",
        document="Pipeline auth finished before the sync completed.",
        categories=[],
        chunk_id=None,
        created_at="2026-06-06T00:00:00+00:00",
        kind=None,
        line_no=None,
        slug="sync-pipeline",
        source_ids=["source-a"],
        title="Sync pipeline",
        updated_at="2026-06-06T00:00:00+00:00",
        version="1",
    )
    claims = generate_wiki_tilegroups.extract_claims_with_llm(
        FakeLlm(),
        doc,
        previous_doc=None,
        now=datetime(2026, 6, 6, tzinfo=timezone.utc),
    )
    assert len(claims) == 1
    claim = claims[0]
    assert claim.claim_type == "summary"
    assert claim.hook_type == "none"
    assert claim.hook_strength == 1.0
    assert claim.deduction_score == 0.0
    assert claim.confidence == 0.0
    assert claim.evidence == ["A", "B"]
    assert claim.entities == ["Connector"]
    assert claim.people == ["Jane Doe"]


def test_tile_with_llm_clamps_score_and_preserves_valid_hook_type() -> None:
    class FakeLlm:
        def json_completion(self, system_prompt: str, user_prompt: str) -> dict:
            return {
                "title": "Auth completes first",
                "body": "Auth is green while sync is still catching up.",
                "hook_type": "not_real",
                "why_click": "It changes rollout expectations.",
                "score": "3.2",
            }

    tile = generate_wiki_tilegroups.tile_with_llm(
        FakeLlm(),
        _claim(hook_type="official_vs_actual"),
        updated_at="2026-06-06T00:00:00+00:00",
        now=datetime(2026, 6, 6, tzinfo=timezone.utc),
    )
    assert tile.hook_type == "official_vs_actual"
    assert tile.score == 1.0


def test_select_docs_for_processing_uses_richest_chunk_per_version() -> None:
    docs = [
        WikiDoc(
            id="doc-v2-title",
            document="# Optimizing Performance\n\n",
            categories=["performance"],
            chunk_id="0",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=0,
            slug="optimizing-performance",
            source_ids=["source-a"],
            title="Optimizing Performance",
            updated_at="2026-06-06T01:00:00+00:00",
            version="2",
        ),
        WikiDoc(
            id="doc-v2-body",
            document="Use smaller collections to keep latency down and pre-warm before traffic spikes.",
            categories=["performance"],
            chunk_id="1",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="optimizing-performance",
            source_ids=["source-a"],
            title="Optimizing Performance",
            updated_at="2026-06-06T01:00:00+00:00",
            version="2",
        ),
        WikiDoc(
            id="doc-v1-body",
            document="Older guidance focused on batching writes.",
            categories=["performance"],
            chunk_id="1",
            created_at="2026-06-05T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="optimizing-performance",
            source_ids=["source-a"],
            title="Optimizing Performance",
            updated_at="2026-06-05T01:00:00+00:00",
            version="1",
        ),
    ]
    selected = select_docs_for_processing(
        docs,
        window_days=7,
        recent_limit=10,
        now=datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc),
    )
    assert len(selected) == 1
    current_doc, previous_doc = selected[0]
    assert current_doc.id == "doc-v2-body"
    assert previous_doc is not None
    assert previous_doc.id == "doc-v1-body"


def test_canonicalize_docs_deduplicates_title_only_chunks() -> None:
    docs = [
        WikiDoc(
            id="title",
            document="# Recruiting Outreach Email Templates\n\n",
            categories=["stub"],
            chunk_id="0",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=0,
            slug="recruiting-outreach-email-templates",
            source_ids=["source-a"],
            title="Recruiting Outreach Email Templates",
            updated_at="2026-06-06T01:00:00+00:00",
            version="1",
        ),
        WikiDoc(
            id="body",
            document="Chroma's outreach strategy targets busy engineers who are skeptical of unsolicited recruitment emails.",
            categories=["stub"],
            chunk_id="1",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="recruiting-outreach-email-templates",
            source_ids=["source-a"],
            title="Recruiting Outreach Email Templates",
            updated_at="2026-06-06T01:00:00+00:00",
            version="1",
        ),
    ]
    canonical = canonicalize_docs(docs)
    assert len(canonical) == 1
    assert canonical[0].id == "body"


def test_global_selection_prefers_high_signal_docs_over_only_recent_docs() -> None:
    docs = [
        WikiDoc(
            id="recent-thin",
            document="Status: completed.",
            categories=["ops"],
            chunk_id="1",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="recent-thin",
            source_ids=["source-a"],
            title="Recent Thin",
            updated_at="2026-06-06T10:00:00+00:00",
            version="1",
        ),
        WikiDoc(
            id="older-rich",
            document=(
                "Owner: Hammad Bashir. The rationale is buried because the official rollout plan "
                "diverged from actual work, and the follow-up bug will affect onboarding soon."
            ),
            categories=["foundation"],
            chunk_id="1",
            created_at="2026-05-20T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="older-rich",
            source_ids=["source-a", "source-b", "source-c"],
            title="Older Rich",
            updated_at="2026-05-25T10:00:00+00:00",
            version="1",
        ),
    ]
    selected = select_docs_for_processing(
        docs,
        mode="global",
        recent_limit=1,
        now=datetime(2026, 6, 6, 12, 0, tzinfo=timezone.utc),
    )
    assert len(selected) == 1
    current_doc, previous_doc = selected[0]
    assert current_doc.id == "older-rich"
    assert previous_doc is None


def test_selection_uses_revision_history_for_previous_doc() -> None:
    live_docs = [
        WikiDoc(
            id="functions-api-v2-body",
            document="The staging Functions API now supports attaching compute functions to collections.",
            categories=["product"],
            chunk_id="1",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="functions-api-staging",
            source_ids=["source-a"],
            title="Functions API (Staging Implementation)",
            updated_at="2026-06-06T10:00:00+00:00",
            version="2",
        )
    ]
    revision_docs = [
        WikiDoc(
            id="functions-api-v1-body::v1",
            document="The staging Functions API allowed a smaller set of collection hooks.",
            categories=["product"],
            chunk_id="1",
            created_at="2026-05-01T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="functions-api-staging",
            source_ids=["source-a"],
            title="Functions API (Staging Implementation)",
            updated_at="2026-05-01T10:00:00+00:00",
            version="1",
        ),
        WikiDoc(
            id="functions-api-v2-body::v2",
            document="The staging Functions API now supports attaching compute functions to collections.",
            categories=["product"],
            chunk_id="1",
            created_at="2026-06-06T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="functions-api-staging",
            source_ids=["source-a"],
            title="Functions API (Staging Implementation)",
            updated_at="2026-06-06T10:00:00+00:00",
            version="2",
        ),
    ]
    selected = select_docs_for_processing(
        live_docs,
        revision_docs=revision_docs,
        mode="global",
        recent_limit=1,
        now=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
    )
    assert len(selected) == 1
    current_doc, previous_doc = selected[0]
    assert current_doc.id == "functions-api-v2-body"
    assert previous_doc is not None
    assert previous_doc.version == "1"


def test_document_signal_score_rewards_meaningful_revision_delta() -> None:
    current_doc = WikiDoc(
        id="functions-api-v2-body",
        document=(
            "The staging Functions API now supports attaching compute functions to collections. "
            "This changes onboarding because teams can wire revision history jobs directly into pipelines."
        ),
        categories=["product"],
        chunk_id="1",
        created_at="2026-06-06T00:00:00+00:00",
        kind="page",
        line_no=2,
        slug="functions-api-staging",
        source_ids=["source-a", "source-b"],
        title="Functions API (Staging Implementation)",
        updated_at="2026-06-06T10:00:00+00:00",
        version="2",
    )
    revision_docs = [
        WikiDoc(
            id="functions-api-v1-body::v1",
            document="The staging Functions API allowed a smaller set of collection hooks.",
            categories=["product"],
            chunk_id="1",
            created_at="2026-05-01T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="functions-api-staging",
            source_ids=["source-a"],
            title="Functions API (Staging Implementation)",
            updated_at="2026-05-01T10:00:00+00:00",
            version="1",
        )
    ]
    unchanged_current = WikiDoc(
        id="steady-state-v2",
        document="The staging Functions API allowed a smaller set of collection hooks.",
        categories=["product"],
        chunk_id="1",
        created_at="2026-06-06T00:00:00+00:00",
        kind="page",
        line_no=2,
        slug="steady-state",
        source_ids=["source-a", "source-b"],
        title="Steady State",
        updated_at="2026-06-06T10:00:00+00:00",
        version="2",
    )
    unchanged_revisions = [
        WikiDoc(
            id="steady-state-v1",
            document="The staging Functions API allowed a smaller set of collection hooks.",
            categories=["product"],
            chunk_id="1",
            created_at="2026-05-01T00:00:00+00:00",
            kind="page",
            line_no=2,
            slug="steady-state",
            source_ids=["source-a"],
            title="Steady State",
            updated_at="2026-05-01T10:00:00+00:00",
            version="1",
        )
    ]

    changed_score = document_signal_score(
        current_doc,
        now=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        revision_history=revision_docs,
    )
    unchanged_score = document_signal_score(
        unchanged_current,
        now=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
        revision_history=unchanged_revisions,
    )

    assert changed_score > unchanged_score


def test_editorialize_current_rewrites_raw_person_thread_titles() -> None:
    tiles = [
        _tile(
            "tile-1",
            slug="sales-leads",
            hook_type="person_involved",
            entities=["Hammad", "Sales"],
            source_ids=["source-a"],
            score=0.9,
        )
    ]
    tiles[0].title = "**Updated:** June 2026 **Owner:** Sales (Hammad, Matt) Chroma"
    tiles[0].body = "**Updated:** June 2026 **Owner:** Sales (Hammad, Matt) Chroma runs automated lead scoring and outbound email campaigns."
    title, body, why = editorialize_current(
        "Updated June 2026 Owner Sales Hammad",
        tiles[0].body,
        "person_thread",
        tiles[0].body,
        tiles,
    )
    assert title != "Updated June 2026 Owner Sales Hammad"
    assert "owner" not in title.lower()
    assert "Hammad" in body
    assert len(body) > 20
    assert len(why) > 10


def test_editorialize_current_prefers_deduction_for_person_thread() -> None:
    tiles = [
        _tile(
            "tile-1",
            slug="startup-credits",
            hook_type="person_involved",
            entities=["Philip", "Credits"],
            source_ids=["source-a"],
            score=0.88,
        ),
        _tile(
            "tile-2",
            slug="startup-credits",
            hook_type="person_involved",
            entities=["Credits"],
            source_ids=["source-a"],
            score=0.85,
        ),
    ]
    tiles[0].title = "Philip Thomas resolved by adding credits"
    tiles[0].body = "Philip Thomas resolved by adding credits after a startup ran into a credits issue."
    tiles[1].title = "**Credits:** $5,000 USD per startup **Status:** Active"
    tiles[1].body = "**Credits:** $5,000 USD per startup **Status:** Active"
    title, body, why = editorialize_current(
        "Credits 5000 USD Per Startup",
        tiles[0].body,
        "person_thread",
        tiles[0].body,
        tiles,
    )
    assert title == "Startup credits are being patched manually"
    assert "Philip" in body
    assert "operational takeaway" in why.lower()


def test_editorialize_current_prefers_deduction_for_official_vs_actual() -> None:
    tiles = [
        _tile(
            "tile-1",
            slug="onboarding",
            hook_type="official_vs_actual",
            entities=["Onboarding"],
            source_ids=["source-a"],
            score=0.9,
        ),
        _tile(
            "tile-2",
            slug="onboarding",
            hook_type="tension",
            entities=["Onboarding", "Trust"],
            source_ids=["source-a"],
            score=0.84,
        ),
    ]
    tiles[0].title = "**The process is expected to take significant time"
    tiles[0].body = "**The process is expected to take significant time and effort**."
    tiles[1].title = "Progress in onboarding is **not timeline-driven** but rather"
    tiles[1].body = "Progress in onboarding is **not timeline-driven** but rather tied to increasing trust and agency."
    title, body, why = editorialize_current(
        "The story and reality may differ",
        tiles[0].body,
        "official_vs_actual",
        tiles[0].body,
        tiles,
    )
    assert title == "Onboarding expects trust to replace timelines"
    assert "trust and agency" in body.lower()
    assert "mismatch" in why.lower()


def test_build_tilegroups_prefers_distinct_entry_slugs() -> None:
    tiles = [
        _tile("tile-1", slug="same-page", hook_type="person_involved", entities=["A"], source_ids=["s1"], score=0.95),
        _tile("tile-2", slug="same-page", hook_type="person_involved", entities=["A"], source_ids=["s1"], score=0.93),
        _tile("tile-3", slug="different-page", hook_type="person_involved", entities=["A"], source_ids=["s2"], score=0.91),
    ]
    groups = build_tilegroups(tiles, now=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc))
    assert len(groups) == 1
    _, selected_tiles = groups[0]
    entry_slugs = [tile.entry_slug for tile in selected_tiles]
    assert entry_slugs[:2] == ["same-page", "different-page"]


def test_select_tiles_for_current_reranks_for_novelty() -> None:
    ranked_tiles = [
        _tile("tile-1", slug="same-page", hook_type="person_involved", entities=["A"], source_ids=["s1"], score=0.95),
        _tile("tile-2", slug="same-page", hook_type="person_involved", entities=["A"], source_ids=["s1"], score=0.94),
        _tile("tile-3", slug="different-page", hook_type="official_vs_actual", entities=["A", "B"], source_ids=["s2"], score=0.90),
    ]

    selected_tiles = select_tiles_for_current(ranked_tiles, limit=2)

    assert [tile.id for tile in selected_tiles] == ["tile-1", "tile-3"]


def test_summarize_output_includes_tile_entrypoints() -> None:
    groups = build_tilegroups(
        [
            _tile("tile-1", slug="rbm-postmortem", hook_type="missing_context", entities=["RBM"], source_ids=["s1"], score=0.9),
            _tile("tile-2", slug="rbm-followup", hook_type="missing_context", entities=["RBM"], source_ids=["s2"], score=0.88),
        ],
        now=datetime(2026, 6, 7, 12, 0, tzinfo=timezone.utc),
    )
    payload = summarize_output(groups, debug={})
    serialized_tile = payload["currents"][0]["tiles"][0]
    assert serialized_tile["entry_slug"] == "rbm-postmortem"
    assert serialized_tile["entry_title"] == "rbm-postmortem page"
    assert serialized_tile["entry_path"] == "/rbm-postmortem"
