use std::{ops::BitAnd, sync::Arc};

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use dashmap::DashMap;
use roaring::RoaringBitmap;
use thiserror::Error;
use uuid::Uuid;

use super::tokenizer::{DocumentTokens, QueryPlan, TokenLookup};

/// Key layout (32 bits): `[partition:2][id:24][chunk:6]`.
///
/// Doc-ID bitmaps (token buckets and transition docs) are chunked into
/// 2^CHUNK_BITS (16M) doc-ID ranges, bounding per-entry bitmap size.
/// The top 2 bits select the partition, placing each in a distinct key range:
///   keys [0, 2^30)           — token bucket doc-ID bitmaps
///   keys [2^30, 2^31)        — transition doc-ID bitmaps
///   keys [2^31, 2^31 + 2^24) — transition bucket-ID bitmaps (not chunked)
const CHUNK_BITS: u32 = 24;
const CHUNK_SHIFT: u32 = 6;
const MAX_CHUNK: u32 = (1 << CHUNK_SHIFT) - 1;
const TRANSITION_DOC_FLAG: u32 = 1 << 30;
const TRANSITION_BUCKET_FLAG: u32 = 1 << 31;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum FullTextBitmapError {
    #[error("Blockfile error: {0}")]
    Blockfile(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FullTextBitmapError {
    fn code(&self) -> ErrorCodes {
        match self {
            FullTextBitmapError::Blockfile(e) => e.code(),
        }
    }
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// Per-bucket accumulator for doc-ID adds and deletes.
#[derive(Default)]
struct TokenDelta {
    adds: RoaringBitmap,
    deletes: RoaringBitmap,
}

/// Per-transition accumulator for doc-ID and bucket-ID bitmaps.
#[derive(Default)]
struct TransitionDelta {
    bucket_adds: RoaringBitmap,
    doc_adds: RoaringBitmap,
    doc_deletes: RoaringBitmap,
}

/// Per-trigram accumulator: one bucket-ID bitmap per positional key.
#[derive(Default)]
struct TrigramDelta {
    infix: RoaringBitmap,
    prefix: RoaringBitmap,
    suffix: RoaringBitmap,
}

/// Writer for the word-based full-text bitmap index.
///
/// Receives pre-computed [`DocumentTokens`] from the tokenizer and writes
/// bucket, trigram, and transition bitmaps to a single blockfile.
///
/// # Blockfile layout
///
/// A single blockfile with four logical partitions under `prefix=""`,
/// plus trigram entries under `prefix="{trigram}"`. Keys are 32 bits
/// laid out as `[partition:2][id:24][chunk:6]` (see constants above).
/// The top 2 bits place each partition in a distinct key range:
///
/// 1. `prefix=""`, keys `[0, 2^30)` — token bucket doc-ID bitmaps (chunked).
///    `key = (bucket_id << CHUNK_SHIFT) | chunk_index`.
/// 2. `prefix=""`, keys `[2^30, 2^31)` — transition doc-ID bitmaps (chunked).
///    `key = TRANSITION_DOC_FLAG | (hash << CHUNK_SHIFT) | chunk_index`.
/// 3. `prefix=""`, keys `[2^31, 2^31 + 2^24)` — transition bucket-ID bitmaps (not chunked).
///    `key = TRANSITION_BUCKET_FLAG | hash`.
/// 4. `prefix="{trigram}"`, keys 0/1/2 — positional bucket-ID bitmaps.
#[derive(Clone)]
pub struct FullTextBitmapWriter {
    bitmap_writer: BlockfileWriter,
    old_reader: Option<FullTextBitmapReader>,
    token_deltas: Arc<DashMap<u32, TokenDelta>>,
    transition_deltas: Arc<DashMap<u32, TransitionDelta>>,
    trigram_deltas: Arc<DashMap<String, TrigramDelta>>,
}

impl FullTextBitmapWriter {
    pub fn new(bitmap_writer: BlockfileWriter, old_reader: Option<FullTextBitmapReader>) -> Self {
        Self {
            bitmap_writer,
            old_reader,
            token_deltas: Arc::new(DashMap::new()),
            transition_deltas: Arc::new(DashMap::new()),
            trigram_deltas: Arc::new(DashMap::new()),
        }
    }

    /// Add a document to the index.
    pub fn add_document(&self, offset_id: u32, tokens: DocumentTokens) {
        for bucket in tokens.buckets {
            let mut entry = self.token_deltas.entry(bucket).or_default();
            entry.deletes.remove(offset_id);
            entry.adds.insert(offset_id);
        }

        for (trigram, key, bucket_id) in tokens.trigrams {
            let mut entry = self.trigram_deltas.entry(trigram).or_default();
            match key {
                0 => entry.prefix.insert(bucket_id),
                1 => entry.infix.insert(bucket_id),
                _ => entry.suffix.insert(bucket_id),
            };
        }

        for (hash, prev_bucket, curr_bucket) in tokens.transitions {
            let mut entry = self.transition_deltas.entry(hash).or_default();
            entry.doc_deletes.remove(offset_id);
            entry.doc_adds.insert(offset_id);
            entry.bucket_adds.insert(prev_bucket);
            entry.bucket_adds.insert(curr_bucket);
        }
    }

    /// Delete a document from the index.
    ///
    /// Removes the doc from bucket and transition doc bitmaps. Trigram and
    /// transition bucket bitmaps are left stale — they are over-estimates.
    pub fn delete_document(&self, offset_id: u32, tokens: DocumentTokens) {
        for bucket in tokens.buckets {
            let mut entry = self.token_deltas.entry(bucket).or_default();
            entry.adds.remove(offset_id);
            entry.deletes.insert(offset_id);
        }

        for (hash, _, _) in tokens.transitions {
            let mut entry = self.transition_deltas.entry(hash).or_default();
            entry.doc_adds.remove(offset_id);
            entry.doc_deletes.insert(offset_id);
        }
    }

    /// Merge accumulated deltas into the blockfile.
    ///
    /// Writes in `(prefix, key)` sorted order as required by the blockfile:
    /// 1. Token buckets — keys `[0, 2^30)`
    /// 2. Transition doc bitmaps — keys `[2^30, 2^31)`
    /// 3. Transition bucket bitmaps — keys `[2^31, 2^31 + 2^24)`
    /// 4. Trigrams (`prefix="{trigram}"`)
    pub async fn write_to_blockfiles(&self) -> Result<(), FullTextBitmapError> {
        self.write_token_buckets().await?;
        self.write_transition_docs().await?;
        self.write_transition_buckets().await?;
        self.write_trigrams().await?;
        Ok(())
    }

    pub async fn commit(self) -> Result<FullTextBitmapFlusher, FullTextBitmapError> {
        let flusher = self.bitmap_writer.commit::<u32, RoaringBitmap>().await?;
        Ok(FullTextBitmapFlusher { flusher })
    }

    // --- Write phases (one per partition, in sorted key order) ---

    /// Partition 1: token bucket doc-ID bitmaps, keys `[0, 2^30)` (chunked).
    async fn write_token_buckets(&self) -> Result<(), FullTextBitmapError> {
        let mut bucket_ids: Vec<u32> = self.token_deltas.iter().map(|e| *e.key()).collect();
        bucket_ids.sort_unstable();

        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(bucket_ids.iter().flat_map(|&b| {
                    (0u32..=MAX_CHUNK).map(move |c| (String::new(), (b << CHUNK_SHIFT) | c))
                }))
                .await;
        }

        for bucket_id in bucket_ids {
            let Some((_, delta)) = self.token_deltas.remove(&bucket_id) else {
                continue;
            };
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_doc_bitmap(0, bucket_id).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &delta.adds;
            bitmap -= &delta.deletes;
            self.write_doc_bitmap(0, bucket_id, &bitmap).await?;
        }
        Ok(())
    }

    /// Partition 4: trigram positional bucket-ID bitmaps (not chunked).
    async fn write_trigrams(&self) -> Result<(), FullTextBitmapError> {
        let mut keys: Vec<String> = self
            .trigram_deltas
            .iter()
            .map(|e| e.key().clone())
            .collect();
        keys.sort_unstable();

        // Preload blocks for all trigram keys (up to 3 positional keys each).
        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(
                    keys.iter()
                        .flat_map(|t| (0u32..3).map(move |k| (t.clone(), k))),
                )
                .await;
        }

        for trigram in keys {
            let Some((_, delta)) = self.trigram_deltas.remove(&trigram) else {
                continue;
            };
            for (key, new_buckets) in [(0u32, delta.prefix), (1, delta.infix), (2, delta.suffix)] {
                if new_buckets.is_empty() {
                    continue;
                }
                let mut bitmap = match &self.old_reader {
                    Some(r) => r
                        .bitmap_reader
                        .get(&trigram, key)
                        .await?
                        .unwrap_or_default(),
                    None => RoaringBitmap::new(),
                };
                bitmap |= &new_buckets;
                self.bitmap_writer
                    .set(trigram.as_str(), key, bitmap)
                    .await?;
            }
        }
        Ok(())
    }

    /// Partition 2: transition doc-ID bitmaps, keys `[2^30, 2^31)` (chunked).
    async fn write_transition_docs(&self) -> Result<(), FullTextBitmapError> {
        let mut hashes: Vec<u32> = self.transition_deltas.iter().map(|e| *e.key()).collect();
        hashes.sort_unstable();

        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(hashes.iter().flat_map(|&h| {
                    (0u32..=MAX_CHUNK)
                        .map(move |c| (String::new(), TRANSITION_DOC_FLAG | (h << CHUNK_SHIFT) | c))
                }))
                .await;
        }

        for &hash in &hashes {
            let Some(entry) = self.transition_deltas.get(&hash) else {
                continue;
            };
            if entry.doc_adds.is_empty() && entry.doc_deletes.is_empty() {
                continue;
            }
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_doc_bitmap(TRANSITION_DOC_FLAG, hash).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &entry.doc_adds;
            bitmap -= &entry.doc_deletes;
            self.write_doc_bitmap(TRANSITION_DOC_FLAG, hash, &bitmap)
                .await?;
        }
        Ok(())
    }

    /// Partition 3: transition bucket-ID bitmaps, keys `[2^31, 2^31 + 2^24)` (not chunked).
    async fn write_transition_buckets(&self) -> Result<(), FullTextBitmapError> {
        let mut hashes: Vec<u32> = self.transition_deltas.iter().map(|e| *e.key()).collect();
        hashes.sort_unstable();

        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(
                    hashes
                        .iter()
                        .map(|&h| (String::new(), TRANSITION_BUCKET_FLAG | h)),
                )
                .await;
        }

        for hash in hashes {
            let Some((_, delta)) = self.transition_deltas.remove(&hash) else {
                continue;
            };
            if delta.bucket_adds.is_empty() {
                continue;
            }
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_id_bitmap(TRANSITION_BUCKET_FLAG | hash).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &delta.bucket_adds;
            self.write_id_bitmap(TRANSITION_BUCKET_FLAG | hash, bitmap)
                .await?;
        }
        Ok(())
    }

    // --- Write helpers ---

    /// Write a doc-ID bitmap, splitting it into 16M-range chunks.
    /// Key encoding: `flag | (id << CHUNK_SHIFT) | chunk_index`.
    async fn write_doc_bitmap(
        &self,
        flag: u32,
        id: u32,
        bitmap: &RoaringBitmap,
    ) -> Result<(), FullTextBitmapError> {
        let docs: Vec<u32> = bitmap.iter().collect();
        for chunk_docs in docs.chunk_by(|a, b| a >> CHUNK_BITS == b >> CHUNK_BITS) {
            let chunk = chunk_docs[0] >> CHUNK_BITS;
            let chunk_bm: RoaringBitmap = chunk_docs.iter().copied().collect();
            self.bitmap_writer
                .set("", flag | (id << CHUNK_SHIFT) | chunk, chunk_bm)
                .await?;
        }
        Ok(())
    }

    /// Write a bounded-universe ID bitmap as a single entry.
    async fn write_id_bitmap(
        &self,
        key: u32,
        bitmap: RoaringBitmap,
    ) -> Result<(), FullTextBitmapError> {
        self.bitmap_writer.set("", key, bitmap).await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Flusher
// ---------------------------------------------------------------------------

pub struct FullTextBitmapFlusher {
    flusher: BlockfileFlusher,
}

impl FullTextBitmapFlusher {
    pub async fn flush(self) -> Result<(), FullTextBitmapError> {
        self.flusher.flush::<u32, RoaringBitmap>().await?;
        Ok(())
    }

    pub fn id(&self) -> Uuid {
        self.flusher.id()
    }

    pub fn prefix_path(&self) -> &str {
        self.flusher.prefix_path()
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct FullTextBitmapReader {
    bitmap_reader: BlockfileReader<'static, u32, RoaringBitmap>,
}

impl FullTextBitmapReader {
    pub fn new(bitmap_reader: BlockfileReader<'static, u32, RoaringBitmap>) -> Self {
        Self { bitmap_reader }
    }

    pub fn id(&self) -> Uuid {
        self.bitmap_reader.id()
    }

    /// Execute a query plan and return candidate doc IDs (over-estimate).
    ///
    /// Stages 1–2 from the ADR:
    /// 1. Resolve each lookup to candidate bucket IDs (direct hash or trigram).
    ///    Filter with transition bitmaps between adjacent tokens.
    /// 2. Load doc bitmaps for surviving buckets, AND across tokens.
    ///
    /// Stage 3 (brute-force verification) is the caller's responsibility.
    pub async fn search(&self, plan: &QueryPlan) -> Result<RoaringBitmap, FullTextBitmapError> {
        if plan.lookups.is_empty() {
            return Ok(RoaringBitmap::new());
        }

        // Load all transition bitmaps upfront.
        self.load_keys(
            plan.transitions
                .iter()
                .flat_map(|&h| {
                    (0u32..=MAX_CHUNK)
                        .map(move |c| (String::new(), TRANSITION_DOC_FLAG | (h << CHUNK_SHIFT) | c))
                })
                .chain(
                    plan.transitions
                        .iter()
                        .map(|&h| (String::new(), TRANSITION_BUCKET_FLAG | h)),
                ),
        )
        .await;

        let mut transitions = Vec::with_capacity(plan.transitions.len());
        for &hash in &plan.transitions {
            transitions.push((
                self.get_id_bitmap(TRANSITION_BUCKET_FLAG | hash).await?,
                self.get_doc_bitmap(TRANSITION_DOC_FLAG, hash).await?,
            ));
        }

        // Stage 1: resolve lookups and apply transition filtering.
        let last_lookup = plan.lookups.len() - 1;
        let mut bucket_sets = Vec::with_capacity(plan.lookups.len());
        let transition_iter = std::iter::once(None).chain(transitions.iter().map(Some));
        for ((i, lookup), transition) in plan.lookups.iter().enumerate().zip(transition_iter) {
            let mut buckets = match lookup {
                TokenLookup::Direct(id) => RoaringBitmap::from([*id]),
                TokenLookup::Trigram(trigrams) => {
                    let is_prefix = i == 0 && !plan.singleton;
                    let is_suffix = i == last_lookup && !plan.singleton;
                    self.trigram_resolve(trigrams, is_prefix, is_suffix).await?
                }
            };
            if let Some((bkt_bm, _)) = transition {
                if let Some(prev) = bucket_sets.last_mut() {
                    *prev &= bkt_bm;
                }
                buckets &= bkt_bm;
            }
            if buckets.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            bucket_sets.push(buckets);
        }

        // Stage 2: load doc bitmaps, AND across tokens + transitions.
        self.load_keys(bucket_sets.iter().flat_map(|bs| {
            bs.iter().flat_map(|id| {
                (0u32..=MAX_CHUNK).map(move |c| (String::new(), (id << CHUNK_SHIFT) | c))
            })
        }))
        .await;

        let mut doc_bitmaps = Vec::with_capacity(bucket_sets.len() + transitions.len());
        for bucket_set in &bucket_sets {
            let mut bm = RoaringBitmap::new();
            for bucket_id in bucket_set {
                bm |= &self.get_doc_bitmap(0, bucket_id).await?;
            }
            if bm.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            doc_bitmaps.push(bm);
        }
        doc_bitmaps.extend(transitions.into_iter().map(|(_, doc_bm)| doc_bm));

        doc_bitmaps.sort_by_key(|bm| bm.len());
        Ok(doc_bitmaps
            .into_iter()
            .reduce(BitAnd::bitand)
            .unwrap_or_default())
    }

    /// Preload blocks for a set of `(prefix, key)` pairs into the cache.
    pub async fn load_keys(&self, keys: impl IntoIterator<Item = (String, u32)>) {
        self.bitmap_reader.load_data_for_keys(keys).await;
    }

    // --- Read helpers ---

    /// Load a doc-ID bitmap, merging all chunks via range read.
    /// Key encoding: `flag | (id << CHUNK_SHIFT) | chunk_index`.
    pub async fn get_doc_bitmap(
        &self,
        flag: u32,
        id: u32,
    ) -> Result<RoaringBitmap, FullTextBitmapError> {
        let lo = flag | (id << CHUNK_SHIFT);
        let hi = lo | MAX_CHUNK;
        let entries = self.bitmap_reader.get_range(""..="", lo..=hi).await?;
        let mut result = RoaringBitmap::new();
        for (_, _, chunk_bm) in entries {
            result |= chunk_bm;
        }
        Ok(result)
    }

    /// Load a bounded-universe ID bitmap via point read.
    pub async fn get_id_bitmap(&self, key: u32) -> Result<RoaringBitmap, FullTextBitmapError> {
        Ok(self.bitmap_reader.get("", key).await?.unwrap_or_default())
    }

    // --- Private helpers ---

    /// Resolve trigrams to candidate bucket IDs via the trigram index.
    ///
    /// For each trigram, loads the appropriate positional keys based on
    /// the token type (prefix/suffix/singleton) and the trigram's position
    /// within the token. ORs within a trigram, ANDs across trigrams.
    async fn trigram_resolve(
        &self,
        trigrams: &[String],
        is_prefix: bool,
        is_suffix: bool,
    ) -> Result<RoaringBitmap, FullTextBitmapError> {
        self.load_keys(
            trigrams
                .iter()
                .flat_map(|t| (0u32..3).map(move |k| (t.clone(), k))),
        )
        .await;

        let last = trigrams.len().saturating_sub(1);
        let mut bitmaps = Vec::with_capacity(trigrams.len());

        for (i, trigram) in trigrams.iter().enumerate() {
            let keys: &[u32] = match (i == 0, i == last) {
                (true, true) => &[0, 1, 2],
                (true, false) => {
                    if is_suffix {
                        &[0]
                    } else {
                        &[0, 1]
                    }
                }
                (false, true) => {
                    if is_prefix {
                        &[2]
                    } else {
                        &[1, 2]
                    }
                }
                (false, false) => &[1],
            };

            let mut bm = RoaringBitmap::new();
            for &key in keys {
                bm |= &self
                    .bitmap_reader
                    .get(trigram, key)
                    .await?
                    .unwrap_or_default();
            }
            bitmaps.push(bm);
        }

        Ok(bitmaps
            .into_iter()
            .reduce(BitAnd::bitand)
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::large_futures)]
mod tests {
    use super::*;
    use crate::fulltext::tokenizer::WordAnalyzer;
    use chroma_blockstore::{
        arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider,
        test_arrow_blockfile_provider, BlockfileWriterOptions,
    };
    use tempfile::TempDir;

    fn tokenize(text: &str) -> DocumentTokens {
        WordAnalyzer::default().tokenize_document(text).unwrap()
    }

    fn provider() -> (TempDir, BlockfileProvider) {
        test_arrow_blockfile_provider(1024 * 1024)
    }

    async fn new_writer(provider: &BlockfileProvider) -> FullTextBitmapWriter {
        let writer = provider
            .write::<u32, RoaringBitmap>(
                BlockfileWriterOptions::new(String::new()).ordered_mutations(),
            )
            .await
            .unwrap();
        FullTextBitmapWriter::new(writer, None)
    }

    async fn flush_and_read(
        writer: FullTextBitmapWriter,
        provider: &BlockfileProvider,
    ) -> FullTextBitmapReader {
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();
        let reader = provider
            .read::<u32, RoaringBitmap>(BlockfileReaderOptions::new(id, String::new()))
            .await
            .unwrap();
        FullTextBitmapReader::new(reader)
    }

    #[tokio::test]
    async fn test_add_and_delete() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let hw_buckets = tokenize("hello world").buckets;
        let hr_buckets = tokenize("hello rust").buckets;

        writer.add_document(1, tokenize("hello world"));
        writer.add_document(2, tokenize("hello rust"));
        writer.delete_document(1, tokenize("hello world"));
        let reader = flush_and_read(writer, &provider).await;

        // "hello" bucket (shared): doc 2 only.
        let hello_bucket = *hw_buckets.iter().find(|b| hr_buckets.contains(b)).unwrap();
        let bm = reader.get_doc_bitmap(0, hello_bucket).await.unwrap();
        assert!(!bm.contains(1));
        assert!(bm.contains(2));

        // "world" bucket (unique to doc 1): empty after delete.
        let world_bucket = *hw_buckets.iter().find(|b| !hr_buckets.contains(b)).unwrap();
        let bm = reader.get_doc_bitmap(0, world_bucket).await.unwrap();
        assert!(bm.is_empty());
    }

    #[tokio::test]
    async fn test_update_as_delete_then_add() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let old_buckets = tokenize("hello world").buckets;
        let new_buckets = tokenize("hello rust").buckets;

        writer.delete_document(1, tokenize("hello world"));
        writer.add_document(1, tokenize("hello rust"));
        let reader = flush_and_read(writer, &provider).await;

        // "hello" in both — add wins.
        let hello = *old_buckets
            .iter()
            .find(|b| new_buckets.contains(b))
            .unwrap();
        assert!(reader.get_doc_bitmap(0, hello).await.unwrap().contains(1));
        // "world" only in old — deleted.
        let world = *old_buckets
            .iter()
            .find(|b| !new_buckets.contains(b))
            .unwrap();
        assert!(!reader.get_doc_bitmap(0, world).await.unwrap().contains(1));
        // "rust" only in new — added.
        let rust = *new_buckets
            .iter()
            .find(|b| !old_buckets.contains(b))
            .unwrap();
        assert!(reader.get_doc_bitmap(0, rust).await.unwrap().contains(1));
    }

    #[tokio::test]
    async fn test_trigram_entries() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let bucket = tokenize("hello").buckets[0];

        writer.add_document(1, tokenize("hello"));
        let reader = flush_and_read(writer, &provider).await;

        // "hel"(key=0), "ell"(key=1), "llo"(key=2)
        assert!(reader
            .bitmap_reader
            .get("hel", 0)
            .await
            .unwrap()
            .unwrap()
            .contains(bucket));
        assert!(reader
            .bitmap_reader
            .get("ell", 1)
            .await
            .unwrap()
            .unwrap()
            .contains(bucket));
        assert!(reader
            .bitmap_reader
            .get("llo", 2)
            .await
            .unwrap()
            .unwrap()
            .contains(bucket));
        // Wrong keys should not contain the bucket.
        assert!(!reader
            .bitmap_reader
            .get("hel", 1)
            .await
            .unwrap()
            .unwrap_or_default()
            .contains(bucket));
        assert!(!reader
            .bitmap_reader
            .get("llo", 0)
            .await
            .unwrap()
            .unwrap_or_default()
            .contains(bucket));
    }

    #[tokio::test]
    async fn test_transition_entries() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let transitions = tokenize("hello world peace").transitions;

        writer.add_document(1, tokenize("hello world peace"));
        let reader = flush_and_read(writer, &provider).await;

        assert_eq!(transitions.len(), 2);
        for (hash, prev_bucket, curr_bucket) in &transitions {
            let doc_bm = reader
                .get_doc_bitmap(TRANSITION_DOC_FLAG, *hash)
                .await
                .unwrap();
            assert!(doc_bm.contains(1));
            let bkt_bm = reader
                .get_id_bitmap(TRANSITION_BUCKET_FLAG | hash)
                .await
                .unwrap();
            assert!(bkt_bm.contains(*prev_bucket));
            assert!(bkt_bm.contains(*curr_bucket));
        }
    }

    #[tokio::test]
    async fn test_delete_cleans_transition_docs() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let transitions = tokenize("hello world").transitions;

        writer.add_document(1, tokenize("hello world"));
        writer.add_document(2, tokenize("hello world"));
        writer.delete_document(1, tokenize("hello world"));
        let reader = flush_and_read(writer, &provider).await;

        for (hash, _, _) in &transitions {
            let doc_bm = reader
                .get_doc_bitmap(TRANSITION_DOC_FLAG, *hash)
                .await
                .unwrap();
            assert!(!doc_bm.contains(1));
            assert!(doc_bm.contains(2));
        }
    }

    #[tokio::test]
    async fn test_fork_with_old_reader() {
        let (_tmp, provider) = provider();
        let hw_buckets = tokenize("hello world").buckets;
        let hr_buckets = tokenize("hello rust").buckets;

        // Gen 1: add docs 1 and 2.
        let w1 = new_writer(&provider).await;
        w1.add_document(1, tokenize("hello world"));
        w1.add_document(2, tokenize("hello rust"));
        let reader1 = flush_and_read(w1, &provider).await;

        // Gen 2: fork, add doc 3, delete doc 1.
        let w2 = {
            let bf = provider
                .write::<u32, RoaringBitmap>(
                    BlockfileWriterOptions::new(String::new())
                        .fork(reader1.id())
                        .ordered_mutations(),
                )
                .await
                .unwrap();
            FullTextBitmapWriter::new(bf, Some(reader1))
        };
        w2.add_document(3, tokenize("hello chroma"));
        w2.delete_document(1, tokenize("hello world"));
        let reader2 = flush_and_read(w2, &provider).await;

        // "hello" bucket: docs 2 and 3, not 1.
        let hello = *hw_buckets.iter().find(|b| hr_buckets.contains(b)).unwrap();
        let bm = reader2.get_doc_bitmap(0, hello).await.unwrap();
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
        assert!(bm.contains(3));
    }

    // --- search ---

    fn query(text: &str) -> QueryPlan {
        WordAnalyzer::default().plan_query(text).unwrap()
    }

    #[tokio::test]
    async fn test_search_exact_word() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        writer.add_document(1, tokenize("hello world"));
        writer.add_document(2, tokenize("goodbye world"));
        let reader = flush_and_read(writer, &provider).await;

        // " hello " — bounded on both sides, Direct lookup.
        let result = reader.search(&query(" hello ")).await.unwrap();
        assert!(result.contains(1));
        assert!(!result.contains(2));
    }

    #[tokio::test]
    async fn test_search_partial_match() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        writer.add_document(1, tokenize("hello world"));
        writer.add_document(2, tokenize("help world"));
        let reader = flush_and_read(writer, &provider).await;

        // "hel" — singleton, trigram resolution.
        let result = reader.search(&query("hel")).await.unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));

        // "hello" — singleton, narrows to just doc 1.
        let result = reader.search(&query("hello")).await.unwrap();
        assert!(result.contains(1));
        // May or may not contain doc 2 depending on trigram collisions.
        // The index is a sieve — false positives are acceptable.
    }

    #[tokio::test]
    async fn test_search_multi_word() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        writer.add_document(1, tokenize("hello beautiful world"));
        writer.add_document(2, tokenize("hello cruel world"));
        let reader = flush_and_read(writer, &provider).await;

        // "hello beautiful world" — prefix + body + suffix.
        let result = reader
            .search(&query("hello beautiful world"))
            .await
            .unwrap();
        assert!(result.contains(1));
        // Doc 2 should not match (different body token + transitions).
    }

    #[tokio::test]
    async fn test_search_no_match() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        writer.add_document(1, tokenize("hello world"));
        let reader = flush_and_read(writer, &provider).await;

        let result = reader.search(&query("zebra")).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_search_after_delete() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        writer.add_document(1, tokenize("hello world"));
        writer.add_document(2, tokenize("hello world"));
        writer.delete_document(1, tokenize("hello world"));
        let reader = flush_and_read(writer, &provider).await;

        let result = reader.search(&query("hello")).await.unwrap();
        assert!(!result.contains(1));
        assert!(result.contains(2));
    }

    /// Verify that doc IDs spanning multiple 16M chunks are written and
    /// read back correctly.
    #[tokio::test]
    async fn test_cross_chunk_doc_ids() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;

        let chunk_size = 1u32 << CHUNK_BITS; // 16M
        writer.add_document(0, tokenize("hello world"));
        writer.add_document(1, tokenize("hello rust"));
        writer.add_document(chunk_size, tokenize("hello world"));
        writer.add_document(chunk_size + 1, tokenize("hello chroma"));
        writer.add_document(chunk_size * 2, tokenize("hello world"));
        let reader = flush_and_read(writer, &provider).await;

        // "hello" appears in all 5 docs across 3 chunks.
        let result = reader.search(&query("hello")).await.unwrap();
        assert!(result.contains(0));
        assert!(result.contains(1));
        assert!(result.contains(chunk_size));
        assert!(result.contains(chunk_size + 1));
        assert!(result.contains(chunk_size * 2));

        // "world" appears in docs 0, chunk_size, chunk_size*2.
        let result = reader.search(&query("world")).await.unwrap();
        assert!(result.contains(0));
        assert!(!result.contains(1));
        assert!(result.contains(chunk_size));
        assert!(!result.contains(chunk_size + 1));
        assert!(result.contains(chunk_size * 2));

        // Multi-word search across chunks.
        let result = reader.search(&query("hello world")).await.unwrap();
        assert!(result.contains(0));
        assert!(result.contains(chunk_size));
        assert!(result.contains(chunk_size * 2));
    }
}
