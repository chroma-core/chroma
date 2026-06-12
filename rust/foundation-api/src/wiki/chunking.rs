//! Markdown chunking for wiki pages.
//!
//! The chunker parses markdown with the `tree-sitter-md` block grammar, emits
//! chunk 0 as the first non-blank line (so title search keeps working), then
//! greedy-packs top-level blocks into chunks no larger than `max_bytes` of
//! UTF-8 text.
//!
//! `foundation-research` also has a legacy one-chunk-per-line strategy for
//! collections written before the tree-sitter chunker existed; foundation-api
//! only ever writes new wiki pages and so only ports the tree-sitter chunker.
//!
//! The invariants the Python tests pin — chunk 0 is always the title line,
//! `chunk_id`s are dense, `join_chunks(chunk(content)) == content` for content
//! whose blank lines are truly empty and which has no trailing blank lines, and
//! no non-empty input ever produces an empty document — are reproduced here and
//! covered by the ported parity tests below.
//!
//! The round-trip is line-reconstructed from each chunk's `line_no`, so it is
//! lossy in two bounded ways that match the Python reference: trailing blank
//! rows are dropped (`"# Title\nBody\n"` round-trips to `"# Title\nBody"`), and
//! whitespace-only blank lines normalize to empty. Content drifts at most once
//! on the first read-modify-write and is stable thereafter, with no semantic
//! markdown change. [`ts_trailing_newline_is_dropped`] locks this boundary.

use chroma_types::{Metadata, MetadataValue};

/// Default UTF-8 byte budget for a packed tree-sitter chunk before its
/// trailing inter-record separator.
pub const DEFAULT_MAX_BYTES: usize = 4096;

/// A single chunk of a wiki page.
///
/// `chunk_id` is the dense sequential index used in the Chroma record id
/// `{slug}-{chunk_id}`; `line_no` is the 0-indexed position of the chunk's
/// first line in the original content (sparse — gaps encode blank lines).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub id: String,
    pub slug: String,
    pub chunk_id: usize,
    pub line_no: usize,
    pub text: String,
}

/// The metadata string value persisted for the tree-sitter chunking strategy.
/// Other readers/writers of the collection key their chunking strategy off this
/// value, so it is part of the on-collection contract.
pub const CHUNKING_STRATEGY: &str = "treesitter-markdown";

/// Per-collection chunking parameters, recoverable from collection metadata
/// so every consumer reproduces the chunker the collection was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkingConfig {
    pub max_bytes: usize,
}

impl Default for ChunkingConfig {
    /// Default for fresh collections: tree-sitter markdown @ 4096B.
    fn default() -> Self {
        Self {
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl ChunkingConfig {
    /// Recovers the chunker from a collection's metadata.
    ///
    /// Only the `max_bytes` budget is configurable — foundation-api always
    /// tree-sitter chunks — so missing or malformed metadata simply yields the
    /// default budget. The `chunking_strategy` marker is ignored on read; it
    /// exists only as on-collection interop for other readers.
    pub fn from_collection_metadata(metadata: Option<&Metadata>) -> Self {
        let max_bytes = metadata
            .and_then(|metadata| metadata.get("chunking_max_bytes"))
            .map(|value| match value {
                MetadataValue::Int(i) => usize::try_from(*i).unwrap_or(DEFAULT_MAX_BYTES),
                MetadataValue::Float(f) if *f >= 0.0 => f.trunc() as usize,
                _ => DEFAULT_MAX_BYTES,
            })
            .unwrap_or(DEFAULT_MAX_BYTES);
        Self { max_bytes }
    }

    /// Serializes this config to collection metadata, including the
    /// `chunking_strategy` marker so other readers recover the tree-sitter
    /// chunker.
    pub fn to_metadata(self) -> Metadata {
        let mut metadata = Metadata::new();
        metadata.insert(
            "chunking_strategy".to_string(),
            MetadataValue::Str(CHUNKING_STRATEGY.to_string()),
        );
        metadata.insert(
            "chunking_max_bytes".to_string(),
            MetadataValue::Int(self.max_bytes as i64),
        );
        metadata
    }
}

/// The Chroma record id for a page chunk: `{slug}-{chunk_id}`.
pub fn chunk_id_for(slug: &str, chunk_id: usize) -> String {
    format!("{slug}-{chunk_id}")
}

/// Chunks `content` for the wiki collection described by `config`.
pub fn chunk_content(slug: &str, content: &str, config: &ChunkingConfig) -> Vec<Chunk> {
    chunk_treesitter_markdown(slug, content, config.max_bytes)
}

/// Chunks markdown by tree-sitter block boundaries, capped at `max_bytes`.
///
/// Chunk 0 is always the first non-blank line of the source verbatim (the
/// "title line"), preserving title-search semantics. The remainder is parsed
/// with the `tree-sitter-md` block grammar; top-level blocks inside
/// `document` / `section` / `list` containers are treated as atomic units and
/// greedy-packed into chunks while each chunk's content stays under
/// `max_bytes` of UTF-8 text before its trailing inter-record separator. Units
/// larger than `max_bytes` fall back to greedy line packing inside the unit.
pub fn chunk_treesitter_markdown(slug: &str, content: &str, max_bytes: usize) -> Vec<Chunk> {
    let lines: Vec<&str> = content.split('\n').collect();
    let title_idx = lines.iter().position(|line| !line.trim().is_empty());

    let Some(title_idx) = title_idx else {
        // Entirely blank / empty content. Emit a single placeholder so every
        // page has the chunk_id=0 row title search relies on.
        return append_inter_chunk_separators(vec![Chunk {
            id: chunk_id_for(slug, 0),
            slug: slug.to_string(),
            chunk_id: 0,
            line_no: 0,
            text: " ".to_string(),
        }]);
    };

    let mut chunks: Vec<Chunk> = vec![Chunk {
        id: chunk_id_for(slug, 0),
        slug: slug.to_string(),
        chunk_id: 0,
        line_no: title_idx,
        text: lines[title_idx].to_string(),
    }];

    let rest_start = title_idx + 1;
    if rest_start >= lines.len() {
        return append_inter_chunk_separators(chunks);
    }

    let rest_lines = &lines[rest_start..];
    if !rest_lines.iter().any(|line| !line.trim().is_empty()) {
        return append_inter_chunk_separators(chunks);
    }

    let rest_bytes = rest_lines.join("\n");
    let tree = parse_markdown(&rest_bytes);
    let blocks = collect_block_units(tree.root_node());

    let ranges = if blocks.is_empty() {
        // Tree-sitter saw no blocks. Fall back to one-line-per-chunk packing
        // over the non-blank lines so we never lose content.
        pack_lines(0, rest_lines.len() - 1, rest_lines, max_bytes)
    } else {
        pack_blocks(&blocks, rest_lines, max_bytes)
    };

    for (next_id, (cs, ce)) in (1usize..).zip(ranges) {
        chunks.push(Chunk {
            id: chunk_id_for(slug, next_id),
            slug: slug.to_string(),
            chunk_id: next_id,
            line_no: rest_start + cs,
            text: rest_lines[cs..=ce].join("\n"),
        });
    }

    append_inter_chunk_separators(chunks)
}

/// Appends the source newlines needed before the next chunk so concatenating
/// records in `chunk_id` order reproduces the original markdown separators.
fn append_inter_chunk_separators(mut chunks: Vec<Chunk>) -> Vec<Chunk> {
    for i in 0..chunks.len().saturating_sub(1) {
        let end_line = chunks[i].line_no + chunks[i].text.matches('\n').count();
        let next_line_no = chunks[i + 1].line_no;
        if next_line_no > end_line {
            let newline_count = next_line_no - end_line;
            chunks[i].text.push_str(&"\n".repeat(newline_count));
        }
    }
    chunks
}

/// Container node types we descend into rather than treating as leaf units.
/// `list` is included so a long bullet list splits between items instead of
/// forcing a single oversized chunk. Anything else is treated as a unit.
fn is_container(kind: &str) -> bool {
    matches!(kind, "document" | "section" | "list")
}

/// Walks the markdown tree and returns `(start_row, end_row)` (both inclusive)
/// for each top-level block, sorted by start row.
fn collect_block_units(root: tree_sitter::Node) -> Vec<(usize, usize)> {
    let mut blocks: Vec<(usize, usize)> = Vec::new();
    visit_block(root, &mut blocks);
    blocks.sort_by_key(|b| b.0);
    blocks
}

fn visit_block(node: tree_sitter::Node, blocks: &mut Vec<(usize, usize)>) {
    if is_container(node.kind()) {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            visit_block(child, blocks);
        }
        return;
    }
    let start_row = node.start_position().row;
    let end_row = inclusive_end_row(node);
    if end_row < start_row {
        return;
    }
    blocks.push((start_row, end_row));
}

/// Converts tree-sitter's exclusive `end_point` to an inclusive end row.
///
/// Block-grammar nodes typically end at a line boundary (`column == 0` on the
/// row after the last content row), but a trailing block with no terminating
/// newline lands mid-row.
fn inclusive_end_row(node: tree_sitter::Node) -> usize {
    let end = node.end_position();
    if end.column == 0 && end.row > 0 {
        end.row - 1
    } else {
        end.row
    }
}

/// UTF-8 byte length of `rest_lines[start..=end].join("\n")`.
fn byte_len(rest_lines: &[&str], start: usize, end: usize) -> usize {
    if start > end {
        return 0;
    }
    let mut total = 0usize;
    for line in &rest_lines[start..=end] {
        total += line.len();
    }
    total += end - start; // one '\n' between adjacent lines
    total
}

/// Greedy-packs tree-sitter block ranges into chunks no larger than
/// `max_bytes`. Two adjacent blocks are merged only if the byte span
/// `rest_lines[first_start..=second_end]` (folding in blank rows between them)
/// still fits; a single block larger than the budget is split via
/// [`pack_lines`].
fn pack_blocks(
    blocks: &[(usize, usize)],
    rest_lines: &[&str],
    max_bytes: usize,
) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut cur: Option<(usize, usize)> = None;

    for &(b_start, b_end) in blocks {
        match cur {
            None => {
                if byte_len(rest_lines, b_start, b_end) > max_bytes {
                    out.extend(pack_lines(b_start, b_end, rest_lines, max_bytes));
                    continue;
                }
                cur = Some((b_start, b_end));
            }
            Some((cur_start, cur_end)) => {
                if byte_len(rest_lines, cur_start, b_end) <= max_bytes {
                    cur = Some((cur_start, b_end));
                    continue;
                }
                // Doesn't fit; flush current and start a new chunk for this block.
                out.push((cur_start, cur_end));
                cur = None;
                if byte_len(rest_lines, b_start, b_end) > max_bytes {
                    out.extend(pack_lines(b_start, b_end, rest_lines, max_bytes));
                } else {
                    cur = Some((b_start, b_end));
                }
            }
        }
    }

    if let Some(range) = cur {
        out.push(range);
    }
    out
}

/// Greedy-packs a contiguous line range into chunks no larger than
/// `max_bytes`. A line whose own byte length exceeds the budget is emitted as
/// a single oversized chunk — splitting inside a line would garble the source.
fn pack_lines(
    start_row: usize,
    end_row: usize,
    rest_lines: &[&str],
    max_bytes: usize,
) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut cur_start = start_row;
    // `None` is the empty-run sentinel (no line accumulated into the current
    // chunk yet).
    let mut cur_end: Option<usize> = None;
    for r in start_row..=end_row {
        let prospective = byte_len(rest_lines, cur_start, r);
        if cur_end.is_some() && prospective > max_bytes {
            out.push((cur_start, cur_end.unwrap()));
            cur_start = r;
            cur_end = Some(r);
        } else {
            cur_end = Some(r);
        }
    }
    if let Some(ce) = cur_end {
        out.push((cur_start, ce));
    }
    out
}

/// Parses markdown block structure with the `tree-sitter-md` block grammar.
fn parse_markdown(text: &str) -> tree_sitter::Tree {
    let mut parser = tree_sitter::Parser::new();
    let language: tree_sitter::Language = tree_sitter_md::LANGUAGE.into();
    parser
        .set_language(&language)
        .expect("loading the tree-sitter-md block grammar should not fail");
    parser
        .parse(text, None)
        .expect("tree-sitter markdown parse should always produce a tree")
}

/// Reassembles chunks back into their original content using each chunk's
/// `line_no` to place its first line; intervening gaps become empty lines so
/// paragraph breaks survive the round-trip.
///
/// Because the output length is derived from the chunks' line spans, blank rows
/// past the last chunk's content are not represented and are dropped, and
/// whitespace-only blank lines come back as empty. See the module docs.
pub fn join_chunks(chunks: &[Chunk]) -> String {
    if chunks.is_empty() {
        return String::new();
    }
    let mut ordered: Vec<&Chunk> = chunks.iter().collect();
    ordered.sort_by_key(|c| c.line_no);
    let mut total = 0usize;
    for c in &ordered {
        let end = c.line_no + c.text.matches('\n').count() + 1;
        if end > total {
            total = end;
        }
    }
    let mut lines = vec![String::new(); total];
    for c in &ordered {
        for (k, ln) in c.text.split('\n').enumerate() {
            lines[c.line_no + k] = ln.to_string();
        }
    }
    lines.join("\n")
}

/// Returns the page title: the first non-blank line with leading `#` markers
/// and surrounding whitespace stripped (empty string if there is none).
pub fn title_from_content(content: &str) -> String {
    for raw in content.split('\n') {
        let s = raw.trim();
        if s.is_empty() {
            continue;
        }
        let stripped = s.trim_start_matches('#').trim();
        if !stripped.is_empty() {
            return stripped.to_string();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn byte_size(text: &str) -> usize {
        text.len()
    }

    fn blank_line_count(content: &str) -> usize {
        content.split('\n').filter(|l| l.trim().is_empty()).count()
    }

    fn concat(chunks: &[Chunk]) -> String {
        chunks.iter().map(|c| c.text.as_str()).collect()
    }

    // --- treesitter: round-trip ---

    fn assert_ts_round_trip(content: &str, max_bytes: usize, concat_check: bool) {
        let chunks = chunk_treesitter_markdown("p", content, max_bytes);
        assert_eq!(join_chunks(&chunks), content);
        if concat_check {
            assert_eq!(concat(&chunks), content);
        }
    }

    #[test]
    fn ts_paragraph_break_preserved() {
        assert_ts_round_trip("# Title\n\nBody", DEFAULT_MAX_BYTES, true);
    }

    #[test]
    fn ts_multiple_blank_lines_preserved() {
        assert_ts_round_trip("# Title\n\n\n\nBody", DEFAULT_MAX_BYTES, true);
    }

    /// Locks the documented round-trip boundary: the line-reconstructed
    /// `join_chunks` cannot represent blank rows past the last chunk, so
    /// trailing newlines are dropped.
    #[test]
    fn ts_trailing_newline_is_dropped() {
        for (input, expected) in [
            ("# Title\nBody\n", "# Title\nBody"),
            ("# Title\n\nBody\n", "# Title\n\nBody"),
            ("# Title\n", "# Title"),
            ("# Title\n\n\n", "# Title"),
        ] {
            let joined = join_chunks(&chunk_treesitter_markdown("p", input, DEFAULT_MAX_BYTES));
            assert_eq!(joined, expected, "trailing newline handling for {input:?}");
            // The dropped form is a fixed point: no further drift on re-chunk.
            let twice = join_chunks(&chunk_treesitter_markdown("p", &joined, DEFAULT_MAX_BYTES));
            assert_eq!(twice, expected, "re-chunking must be stable for {input:?}");
        }
    }

    /// Whitespace-only blank lines normalize to empty on round-trip, matching
    /// CommonMark blank-line semantics and the Python reference.
    #[test]
    fn ts_whitespace_only_blank_line_normalizes() {
        let joined = join_chunks(&chunk_treesitter_markdown(
            "p",
            "# Title\n  \nBody",
            DEFAULT_MAX_BYTES,
        ));
        assert_eq!(joined, "# Title\n\nBody");
    }

    #[test]
    fn ts_heading_then_body() {
        assert_ts_round_trip(
            "# Title\n\nBody paragraph.\n\nSecond paragraph.",
            DEFAULT_MAX_BYTES,
            true,
        );
    }

    #[test]
    fn ts_fenced_code_block() {
        assert_ts_round_trip(
            "# Title\n\nbefore\n\n```python\nx = 1\ny = 2\n```\n\nafter",
            DEFAULT_MAX_BYTES,
            true,
        );
    }

    #[test]
    fn ts_blank_line_inside_fence() {
        assert_ts_round_trip("# Title\n\n```py\nfoo\n\nbar\n```", DEFAULT_MAX_BYTES, true);
    }

    #[test]
    fn ts_indented_lines_not_stripped() {
        assert_ts_round_trip(
            "# Title\n\n- item\n    continuation\n- next",
            DEFAULT_MAX_BYTES,
            true,
        );
    }

    #[test]
    fn ts_nested_sections() {
        assert_ts_round_trip(
            "# Top\n\n## Section A\n\nBody A.\n\n## Section B\n\nBody B.",
            DEFAULT_MAX_BYTES,
            true,
        );
    }

    #[test]
    fn ts_pack_smaller_than_max_bytes() {
        assert_ts_round_trip("# Title\n\nA\n\nB\n\nC\n\nD", DEFAULT_MAX_BYTES, true);
    }

    #[test]
    fn ts_pack_splits_when_blocks_exceed_budget() {
        let paras: Vec<String> = (0..10).map(|i| format!("Para {i}.")).collect();
        let content = format!("# Title\n\n{}", paras.join("\n\n"));
        assert_ts_round_trip(&content, 16, true);
    }

    // --- treesitter: chunk zero ---

    #[test]
    fn ts_chunk_0_is_title_line() {
        let chunks = chunk_treesitter_markdown("p", "# Title\n\nBody", DEFAULT_MAX_BYTES);
        assert_eq!(chunks[0].chunk_id, 0);
        assert_eq!(chunks[0].id, "p-0");
        assert_eq!(chunks[0].text, "# Title\n\n");
        assert_eq!(chunks[0].line_no, 0);
    }

    #[test]
    fn ts_chunk_0_with_leading_blanks() {
        let chunks = chunk_treesitter_markdown("p", "\n\n# Title\n\nBody", DEFAULT_MAX_BYTES);
        assert_eq!(chunks[0].text, "# Title\n\n");
        assert_eq!(chunks[0].line_no, 2);
    }

    #[test]
    fn ts_chunk_0_when_first_line_is_paragraph() {
        let chunks =
            chunk_treesitter_markdown("p", "Just a paragraph.\n\nMore text.", DEFAULT_MAX_BYTES);
        assert_eq!(chunks[0].text, "Just a paragraph.\n\n");
    }

    #[test]
    fn ts_empty_content_yields_placeholder() {
        let chunks = chunk_treesitter_markdown("p", "", DEFAULT_MAX_BYTES);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, 0);
        assert_ne!(chunks[0].text, "");
    }

    #[test]
    fn ts_blank_content_yields_placeholder() {
        let chunks = chunk_treesitter_markdown("p", "\n\n\n", DEFAULT_MAX_BYTES);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].chunk_id, 0);
        assert_ne!(chunks[0].text, "");
    }

    #[test]
    fn ts_title_only_content() {
        let chunks = chunk_treesitter_markdown("p", "# Title", DEFAULT_MAX_BYTES);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "# Title");
    }

    #[test]
    fn ts_dense_chunk_ids() {
        let chunks = chunk_treesitter_markdown("p", "# Title\n\nA\n\nB\n\nC", 4);
        assert_eq!(
            chunks.iter().map(|c| c.chunk_id).collect::<Vec<_>>(),
            (0..chunks.len()).collect::<Vec<_>>()
        );
        assert_eq!(
            chunks.iter().map(|c| c.id.clone()).collect::<Vec<_>>(),
            (0..chunks.len())
                .map(|i| format!("p-{i}"))
                .collect::<Vec<_>>()
        );
    }

    // --- treesitter: max_bytes budget ---

    #[test]
    fn ts_chunks_fit_max_bytes_when_blocks_are_small() {
        let blocks: Vec<String> = (0..20).map(|i| format!("Block {i}.")).collect();
        let content = format!("# Title\n\n{}", blocks.join("\n\n"));
        let chunks = chunk_treesitter_markdown("p", &content, 64);
        for c in &chunks[1..] {
            assert!(
                byte_size(c.text.trim_end_matches('\n')) <= 64,
                "chunk {} text exceeds budget: {:?}",
                c.chunk_id,
                c.text
            );
        }
    }

    #[test]
    fn ts_oversized_single_line_emits_one_chunk() {
        let long_line = "x".repeat(200);
        let content = format!("# Title\n\n{long_line}");
        let chunks = chunk_treesitter_markdown("p", &content, 64);
        assert_eq!(join_chunks(&chunks), content);
        let body_texts: Vec<&str> = chunks[1..].iter().map(|c| c.text.as_str()).collect();
        assert!(body_texts.contains(&long_line.as_str()));
    }

    #[test]
    fn ts_block_larger_than_max_falls_back_to_lines() {
        let para: Vec<String> = (0..20).map(|i| format!("line {i} of paragraph")).collect();
        let content = format!("# Title\n\n{}", para.join("\n"));
        let chunks = chunk_treesitter_markdown("p", &content, 40);
        for c in &chunks[1..] {
            assert!(byte_size(&c.text) <= 40);
        }
    }

    #[test]
    fn ts_packs_consecutive_small_blocks() {
        let paras: Vec<String> = (0..10).map(|i| format!("P{i}")).collect();
        let content = format!("# Title\n\n{}", paras.join("\n\n"));
        let chunks = chunk_treesitter_markdown("p", &content, DEFAULT_MAX_BYTES);
        assert!(chunks.len() <= 3);
    }

    // --- treesitter: authored-breaks regression ---

    #[test]
    fn ts_metadata_block_with_blank_lines_round_trips() {
        let content = concat_lines(&[
            "# ADR: Cache Invalidation",
            "",
            "**Date:** 2024-04-25",
            "",
            "**Status:** Proposed",
            "",
            "**Source:** [Title](https://example.com)",
            "",
            "**Location:** Engineering Hub / ADR",
            "",
            "## Context",
            "",
            "Body text.",
        ]);
        assert_ts_round_trip(&content, DEFAULT_MAX_BYTES, false);
        let joined = join_chunks(&chunk_treesitter_markdown("p", &content, DEFAULT_MAX_BYTES));
        assert!(joined.contains("**Date:** 2024-04-25\n\n**Status:** Proposed"));
    }

    #[test]
    fn ts_metadata_block_without_blank_lines_round_trips() {
        let content = concat_lines(&[
            "# ADR: Cache Invalidation",
            "",
            "**Date:** 2024-04-25",
            "**Status:** Proposed",
            "**Source:** [Title](https://example.com)",
            "**Location:** Engineering Hub / ADR",
            "",
            "## Context",
            "",
            "Body text.",
        ]);
        assert_ts_round_trip(&content, DEFAULT_MAX_BYTES, false);
        let joined = join_chunks(&chunk_treesitter_markdown("p", &content, DEFAULT_MAX_BYTES));
        assert!(
            joined.contains("**Date:** 2024-04-25\n**Status:** Proposed"),
            "single newlines between metadata lines must survive round-trip"
        );
        assert!(
            !joined.contains("**Date:** 2024-04-25\n\n**Status:**"),
            "chunker must not fabricate blank lines that weren't in the source"
        );
    }

    #[test]
    fn ts_blank_line_count_invariant() {
        for content in [
            "# T\n\nA\n\nB",
            "# T\n\nA\nB\nC",
            "# T\n\n\n\nA",
            "# T\n\nA\n\n\nB\n\nC",
        ] {
            let joined = join_chunks(&chunk_treesitter_markdown("p", content, DEFAULT_MAX_BYTES));
            assert_eq!(
                blank_line_count(&joined),
                blank_line_count(content),
                "blank-line count must survive round-trip for {content:?}"
            );
        }
    }

    #[test]
    fn ts_real_adr_payload_from_trajectory() {
        let content = concat_lines(&[
            "# ADR: Cache Invalidation and Blockfile Reload",
            "",
            "**Date:** 2024-04-25",
            "**Status:** Proposed",
            "**Source:** [Cache Invalidation and Blockfile Reload](https://www.notion.so/foo)",
            "**Location:** Engineering Hub / Architecture Decision Records / ADR",
            "",
            "## Context",
            "",
            "Distributed Chroma uses a **read/write separation architecture** where data for a collection can be spread across multiple query nodes.[^1]",
            "",
            "- **System metadata cache** — system catalog",
            "- **Log record cache** — for brute-force KNN operator",
            "- **Blockfile cache** — cached blockfile data for query nodes",
            "",
            "[^1]: [Cache Invalidation](https://www.notion.so/foo)",
        ]);
        assert_ts_round_trip(&content, DEFAULT_MAX_BYTES, false);
    }

    fn concat_lines(lines: &[&str]) -> String {
        lines.join("\n")
    }

    // --- ChunkingConfig ---

    #[test]
    fn config_default_max_bytes() {
        assert_eq!(ChunkingConfig::default().max_bytes, 4096);
    }

    #[test]
    fn config_metadata_round_trip() {
        let cfg = ChunkingConfig { max_bytes: 2048 };
        let recovered = ChunkingConfig::from_collection_metadata(Some(&cfg.to_metadata()));
        assert_eq!(recovered, cfg);
    }

    #[test]
    fn config_to_metadata_records_strategy_marker() {
        let metadata = ChunkingConfig::default().to_metadata();
        assert_eq!(
            metadata.get("chunking_strategy"),
            Some(&MetadataValue::Str("treesitter-markdown".to_string()))
        );
    }

    #[test]
    fn config_missing_metadata_uses_default_budget() {
        assert_eq!(
            ChunkingConfig::from_collection_metadata(None),
            ChunkingConfig::default()
        );
        assert_eq!(
            ChunkingConfig::from_collection_metadata(Some(&Metadata::new())),
            ChunkingConfig::default()
        );
        let mut unrelated = Metadata::new();
        unrelated.insert(
            "unrelated".to_string(),
            MetadataValue::Str("value".to_string()),
        );
        assert_eq!(
            ChunkingConfig::from_collection_metadata(Some(&unrelated)),
            ChunkingConfig::default()
        );
    }

    #[test]
    fn config_reads_max_bytes_regardless_of_strategy_marker() {
        // The strategy marker is ignored on read; only the budget matters.
        let mut metadata = Metadata::new();
        metadata.insert(
            "chunking_strategy".to_string(),
            MetadataValue::Str("future-thing".to_string()),
        );
        metadata.insert("chunking_max_bytes".to_string(), MetadataValue::Int(1024));
        assert_eq!(
            ChunkingConfig::from_collection_metadata(Some(&metadata)),
            ChunkingConfig { max_bytes: 1024 }
        );
    }

    #[test]
    fn config_chunk_content_tree_sitter_chunks() {
        let chunks = chunk_content("p", "# Title\n\nBody", &ChunkingConfig::default());
        assert_eq!(chunks[0].text, "# Title\n\n");
        assert_eq!(chunks[0].chunk_id, 0);
    }

    #[test]
    fn title_from_content_strips_heading_markers() {
        assert_eq!(title_from_content("# Title\n\nBody"), "Title");
        assert_eq!(title_from_content("\n\n## Nested ##\nx"), "Nested ##");
        assert_eq!(title_from_content("plain line"), "plain line");
        assert_eq!(title_from_content("\n\n   \n"), "");
    }
}
