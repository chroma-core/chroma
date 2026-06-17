"use client";

import { useDeferredValue, useMemo, useState } from "react";

type CurrentTile = {
  title: string;
  body: string;
  hook_type: string;
  score: number;
  entry_slug: string;
  entry_title: string;
  entry_path: string;
  source_ids: string[];
  claim_ids: string[];
};

type CurrentGroup = {
  title: string;
  body: string;
  score: number;
  narrative_type: string;
  salience_score: number;
  hook_quality_score: number;
  evidence: string[];
  tiles: CurrentTile[];
};

type CurrentsPayload = {
  currents: CurrentGroup[];
  debug: {
    docs_scanned: number;
    revision_docs_scanned: number;
    docs_processed: number;
    claims_extracted: number;
    claims_kept: number;
    claims_rejected: number;
    tiles_generated: number;
    groups_generated: number;
    window_days: number;
    selection_mode: string;
    recent_limit: number;
    generated_at: string;
    llm_used: boolean;
  };
};

type TileEntrypoint = {
  entry_slug: string;
  entry_title: string;
  entry_path: string;
  topScore: number;
  topHookType: string;
  supportingTiles: CurrentTile[];
};

const narrativeTone: Record<string, string> = {
  missing_context: "bg-amber-100 text-amber-900",
  official_vs_actual: "bg-rose-100 text-rose-900",
  person_thread: "bg-sky-100 text-sky-900",
  unresolved_tension: "bg-orange-100 text-orange-900",
  near_term_impact: "bg-emerald-100 text-emerald-900",
  buried_rationale: "bg-fuchsia-100 text-fuchsia-900",
  change: "bg-indigo-100 text-indigo-900",
  emerging_storyline: "bg-stone-200 text-stone-900",
};

const narrativeLabels: Record<string, string> = {
  missing_context: "Missing Context",
  official_vs_actual: "Official vs Actual",
  person_thread: "Person Thread",
  unresolved_tension: "Unresolved Tension",
  near_term_impact: "Near-Term Impact",
  buried_rationale: "Buried Rationale",
  change: "Change",
  emerging_storyline: "Emerging Storyline",
};

function formatScore(score: number) {
  return `${Math.round(score * 100)}%`;
}

function titleCaseFromSnakeCase(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function groupTilesByEntrypoint(tiles: CurrentTile[]): TileEntrypoint[] {
  const grouped = new Map<string, TileEntrypoint>();

  for (const tile of tiles) {
    const key = tile.entry_path || tile.entry_slug || tile.title;
    const existing = grouped.get(key);
    if (existing) {
      const becomesTop = tile.score > existing.topScore;
      existing.supportingTiles.push(tile);
      existing.topScore = Math.max(existing.topScore, tile.score);
      if (becomesTop) {
        existing.topHookType = tile.hook_type;
      }
      continue;
    }
    grouped.set(key, {
      entry_slug: tile.entry_slug,
      entry_title: tile.entry_title,
      entry_path: tile.entry_path,
      topScore: tile.score,
      topHookType: tile.hook_type,
      supportingTiles: [tile],
    });
  }

  return Array.from(grouped.values())
    .map((group) => ({
      ...group,
      supportingTiles: group.supportingTiles.sort((left, right) => right.score - left.score),
    }))
    .sort((left, right) => right.topScore - left.topScore);
}

export default function CurrentsBrowser({
  payload,
}: {
  payload: CurrentsPayload;
}) {
  const [query, setQuery] = useState("");
  const [narrativeFilter, setNarrativeFilter] = useState<string>("all");
  const deferredQuery = useDeferredValue(query);

  const filteredCurrents = useMemo(() => {
    const lowered = deferredQuery.trim().toLowerCase();
    return payload.currents.filter((current) => {
      const matchesNarrative =
        narrativeFilter === "all" || current.narrative_type === narrativeFilter;
      if (!matchesNarrative) {
        return false;
      }
      if (!lowered) {
        return true;
      }
      const haystack = [
        current.title,
        current.body,
        current.narrative_type,
        ...current.evidence,
        ...current.tiles.flatMap((tile) => [
          tile.title,
          tile.body,
          tile.entry_title,
          tile.entry_slug,
        ]),
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(lowered);
    });
  }, [deferredQuery, narrativeFilter, payload.currents]);

  const [selectedIndex, setSelectedIndex] = useState(0);
  const safeIndex =
    filteredCurrents.length === 0
      ? -1
      : Math.min(selectedIndex, filteredCurrents.length - 1);
  const selectedCurrent =
    safeIndex >= 0 ? filteredCurrents[safeIndex] : null;
  const selectedEntrypoints = useMemo(
    () => (selectedCurrent ? groupTilesByEntrypoint(selectedCurrent.tiles) : []),
    [selectedCurrent],
  );

  const narrativeOptions = useMemo(() => {
    return Array.from(
      new Set(payload.currents.map((current) => current.narrative_type)),
    );
  }, [payload.currents]);

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(203,213,225,0.45),_transparent_28%),linear-gradient(180deg,_#f4efe3_0%,_#f7f7f2_48%,_#efe8da_100%)] text-stone-900">
      <div className="mx-auto flex min-h-screen max-w-7xl flex-col px-6 py-8 lg:px-10">
        <header className="mb-6 grid gap-5 rounded-[2rem] border border-stone-200/70 bg-white/80 p-6 shadow-[0_20px_80px_rgba(120,113,108,0.12)] backdrop-blur md:grid-cols-[1.2fr_0.8fr]">
          <div className="space-y-3">
            <p className="text-xs font-semibold uppercase tracking-[0.28em] text-stone-500">
              Foundation Currents
            </p>
            <h1 className="max-w-3xl font-serif text-4xl leading-tight tracking-tight text-stone-950">
              Browse deduction-first tile groups and the wiki entrypoints behind
              them.
            </h1>
            <p className="max-w-2xl text-sm leading-6 text-stone-600">
              This explorer surfaces the grouped Currents, their supporting
              subtiles, and the exact wiki pages each tile should send a reader
              into.
            </p>
          </div>

          <div className="grid gap-3 rounded-[1.5rem] bg-stone-950 p-5 text-stone-50">
            <div className="grid grid-cols-2 gap-3 text-sm">
              <Metric
                label="Currents"
                value={String(payload.debug.groups_generated)}
              />
              <Metric
                label="Tiles"
                value={String(payload.debug.tiles_generated)}
              />
              <Metric
                label="Docs Processed"
                value={String(payload.debug.docs_processed)}
              />
              <Metric
                label="Selection"
                value={titleCaseFromSnakeCase(payload.debug.selection_mode)}
              />
            </div>
            <p className="text-xs leading-5 text-stone-300">
              Generated {new Date(payload.debug.generated_at).toLocaleString()}
              {" · "}
              {payload.debug.window_days}-day window
              {" · "}
              {payload.debug.llm_used ? "LLM-assisted" : "Heuristic pass"}
            </p>
          </div>
        </header>

        <div className="mb-5 grid gap-3 rounded-[1.5rem] border border-stone-200/70 bg-white/80 p-4 shadow-[0_10px_40px_rgba(120,113,108,0.08)] backdrop-blur md:grid-cols-[1fr_auto] md:items-center">
          <label className="flex items-center gap-3 rounded-2xl border border-stone-200 bg-stone-50 px-4 py-3">
            <span className="text-xs font-semibold uppercase tracking-[0.22em] text-stone-500">
              Search
            </span>
            <input
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setSelectedIndex(0);
              }}
              placeholder="RBM, onboarding, Hammad, credits..."
              className="w-full bg-transparent text-sm text-stone-900 outline-none placeholder:text-stone-400"
            />
          </label>

          <div className="flex flex-wrap items-center gap-2">
            <FilterChip
              active={narrativeFilter === "all"}
              label="All narratives"
              onClick={() => {
                setNarrativeFilter("all");
                setSelectedIndex(0);
              }}
            />
            {narrativeOptions.map((option) => (
              <FilterChip
                key={option}
                active={narrativeFilter === option}
                label={narrativeLabels[option] ?? titleCaseFromSnakeCase(option)}
                onClick={() => {
                  setNarrativeFilter(option);
                  setSelectedIndex(0);
                }}
              />
            ))}
          </div>
        </div>

        <div className="grid flex-1 gap-5 lg:grid-cols-[0.95fr_1.35fr]">
          <aside className="overflow-hidden rounded-[1.8rem] border border-stone-200/70 bg-white/85 shadow-[0_18px_70px_rgba(120,113,108,0.09)] backdrop-blur">
            <div className="border-b border-stone-200/70 px-5 py-4">
              <p className="text-sm font-medium text-stone-600">
                {filteredCurrents.length} currents
              </p>
            </div>
            <div className="max-h-[calc(100vh-22rem)] overflow-y-auto">
              {filteredCurrents.length === 0 ? (
                <div className="px-5 py-10 text-sm text-stone-500">
                  No currents match this filter yet.
                </div>
              ) : (
                filteredCurrents.map((current, index) => {
                  const active = index === safeIndex;
                  return (
                    <button
                      key={`${current.title}-${index}`}
                      type="button"
                      onClick={() => setSelectedIndex(index)}
                      className={`grid w-full gap-3 border-b border-stone-200/70 px-5 py-4 text-left transition ${
                        active
                          ? "bg-stone-950 text-stone-50"
                          : "bg-transparent text-stone-900 hover:bg-stone-100/80"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <span
                          className={`inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] ${
                            active
                              ? "bg-stone-50/15 text-stone-100"
                              : narrativeTone[current.narrative_type] ??
                                "bg-stone-200 text-stone-900"
                          }`}
                        >
                          {narrativeLabels[current.narrative_type] ??
                            titleCaseFromSnakeCase(current.narrative_type)}
                        </span>
                        <span className="text-xs font-medium text-inherit/70">
                          {formatScore(current.score)}
                        </span>
                      </div>
                      <div className="space-y-2">
                        <h2 className="text-lg font-semibold leading-snug">
                          {current.title}
                        </h2>
                        <p className="line-clamp-3 text-sm leading-6 text-inherit/75">
                          {current.body}
                        </p>
                      </div>
                    </button>
                  );
                })
              )}
            </div>
          </aside>

          <section className="overflow-hidden rounded-[1.8rem] border border-stone-200/70 bg-white/88 shadow-[0_18px_70px_rgba(120,113,108,0.09)] backdrop-blur">
            {selectedCurrent ? (
              <div className="grid h-full grid-rows-[auto_auto_1fr]">
                <div className="border-b border-stone-200/70 px-6 py-6">
                  <div className="mb-4 flex flex-wrap items-center gap-2">
                    <span
                      className={`inline-flex rounded-full px-3 py-1 text-xs font-semibold uppercase tracking-[0.22em] ${
                        narrativeTone[selectedCurrent.narrative_type] ??
                        "bg-stone-200 text-stone-900"
                      }`}
                    >
                      {narrativeLabels[selectedCurrent.narrative_type] ??
                        titleCaseFromSnakeCase(selectedCurrent.narrative_type)}
                    </span>
                    <ScorePill
                      label="Salience"
                      value={formatScore(selectedCurrent.salience_score)}
                    />
                    <ScorePill
                      label="Hook"
                      value={formatScore(selectedCurrent.hook_quality_score)}
                    />
                  </div>
                  <h2 className="max-w-4xl font-serif text-3xl leading-tight tracking-tight text-stone-950">
                    {selectedCurrent.title}
                  </h2>
                  <p className="mt-3 max-w-3xl text-base leading-7 text-stone-600">
                    {selectedCurrent.body}
                  </p>
                </div>

                <div className="border-b border-stone-200/70 bg-stone-50/80 px-6 py-5">
                  <h3 className="mb-3 text-sm font-semibold uppercase tracking-[0.18em] text-stone-500">
                    Why this grouped current exists
                  </h3>
                  <div className="flex flex-wrap gap-2">
                    {selectedCurrent.evidence.map((item) => (
                      <span
                        key={item}
                        className="rounded-full border border-stone-200 bg-white px-3 py-1.5 text-sm text-stone-600"
                      >
                        {item}
                      </span>
                    ))}
                  </div>
                </div>

                <div className="grid gap-4 overflow-y-auto px-6 py-6">
                  <div className="flex items-center justify-between gap-3">
                    <h3 className="text-sm font-semibold uppercase tracking-[0.18em] text-stone-500">
                      Supporting entrypoints
                    </h3>
                    <p className="text-xs text-stone-500">
                      Each card is a wiki page with supporting tile bullets
                    </p>
                  </div>
                  {selectedEntrypoints.map((entrypoint) => (
                    <article
                      key={entrypoint.entry_path}
                      className="grid gap-4 rounded-[1.5rem] border border-stone-200 bg-stone-50/60 p-5"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="space-y-2">
                          <span className="inline-flex rounded-full bg-white px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.2em] text-stone-500">
                            {entrypoint.topHookType.replace(/_/g, " ")}
                          </span>
                          <h4 className="text-lg font-semibold leading-snug text-stone-950">
                            {entrypoint.entry_title}
                          </h4>
                        </div>
                        <div className="rounded-2xl bg-stone-950 px-3 py-2 text-right text-stone-50">
                          <div className="text-[11px] uppercase tracking-[0.18em] text-stone-300">
                            Top score
                          </div>
                          <div className="text-sm font-semibold">
                            {formatScore(entrypoint.topScore)}
                          </div>
                        </div>
                      </div>

                      <div className="grid gap-3">
                        {entrypoint.supportingTiles.map((tile) => (
                          <div
                            key={`${entrypoint.entry_path}-${tile.title}`}
                            className="rounded-[1.1rem] border border-stone-200/80 bg-white/80 p-4"
                          >
                            <div className="mb-2 flex flex-wrap items-center gap-2">
                              <span className="inline-flex rounded-full bg-stone-100 px-2 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-500">
                                {tile.hook_type.replace(/_/g, " ")}
                              </span>
                              <span className="text-xs text-stone-400">
                                {formatScore(tile.score)}
                              </span>
                            </div>
                            <h5 className="text-sm font-semibold leading-6 text-stone-900">
                              {tile.title}
                            </h5>
                            <p className="mt-1 text-sm leading-6 text-stone-600">
                              {tile.body}
                            </p>
                          </div>
                        ))}
                      </div>

                      <div className="grid gap-3 rounded-[1.2rem] border border-stone-200 bg-white p-4 md:grid-cols-[1fr_auto] md:items-center">
                        <div className="space-y-1">
                          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-stone-500">
                            Wiki entrypoint
                          </p>
                          <p className="font-medium text-stone-900">
                            {entrypoint.entry_title}
                          </p>
                          <code className="text-xs text-stone-500">
                            {entrypoint.entry_path}
                          </code>
                        </div>
                        <a
                          href={entrypoint.entry_path}
                          className="inline-flex items-center justify-center rounded-full bg-stone-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-stone-700"
                        >
                          Open entrypoint
                        </a>
                      </div>
                    </article>
                  ))}
                </div>
              </div>
            ) : (
              <div className="flex h-full items-center justify-center px-6 py-24 text-stone-500">
                Pick a current to inspect its tile group and wiki entrypoints.
              </div>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-2xl border border-stone-800 bg-stone-900/70 px-3 py-3">
      <div className="text-[11px] uppercase tracking-[0.2em] text-stone-400">
        {label}
      </div>
      <div className="mt-1 text-base font-semibold text-stone-50">{value}</div>
    </div>
  );
}

function FilterChip({
  active,
  label,
  onClick,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`rounded-full px-3 py-2 text-sm transition ${
        active
          ? "bg-stone-950 text-white"
          : "bg-stone-100 text-stone-600 hover:bg-stone-200"
      }`}
    >
      {label}
    </button>
  );
}

function ScorePill({ label, value }: { label: string; value: string }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-stone-200 bg-white px-3 py-1.5 text-xs font-medium text-stone-600">
      <span className="uppercase tracking-[0.16em] text-stone-400">{label}</span>
      <span className="text-stone-900">{value}</span>
    </span>
  );
}
