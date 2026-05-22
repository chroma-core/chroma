use std::sync::Arc;

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use dashmap::DashMap;
use roaring::RoaringBitmap;
use thiserror::Error;
use uuid::Uuid;

use super::tokenizer::DocumentTokens;

/// Transition doc-ID keys: `key = hash | (1 << 24)`, range `[2^24, 2^25)`.
const TRANSITION_DOC_FLAG: u32 = 1 << 24;
/// Transition bucket-ID keys: `key = hash | (1 << 25)`, range `[2^25, 2^25 + 2^24)`.
const TRANSITION_BUCKET_FLAG: u32 = 1 << 25;

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
/// A single blockfile with three logical partitions, distinguished by
/// prefix and key range. The blockfile writer requires `(prefix, key)`
/// pairs in globally sorted order, so entries are written as:
///
/// 1. `prefix=""`, token bucket keys `[0, 2^24)` — doc-ID bitmaps
/// 2. `prefix=""`, transition keys `[2^24, 2^26)` — doc-ID and bucket-ID bitmaps
/// 3. `prefix="{trigram}"`, keys 0/1/2 — bucket-ID bitmaps (positional)
///
/// This ordering ensures all `prefix=""` entries (partitions 1–2) are
/// written first in ascending key order, followed by trigram entries
/// sorted by `(prefix, key)`.
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
    /// 1. Token buckets: `prefix=""`, keys `[0, 2^24)`
    /// 2. Transitions: `prefix=""`, keys `[2^24, 2^26)`
    /// 3. Trigrams: `prefix="{trigram}"`, keys `0/1/2`
    pub async fn write_to_blockfiles(&self) -> Result<(), FullTextBitmapError> {
        self.write_token_buckets().await?;
        self.write_transitions().await?;
        self.write_trigrams().await?;
        Ok(())
    }

    pub async fn commit(self) -> Result<FullTextBitmapFlusher, FullTextBitmapError> {
        let flusher = self.bitmap_writer.commit::<u32, RoaringBitmap>().await?;
        Ok(FullTextBitmapFlusher { flusher })
    }

    // --- Phases ---

    async fn write_token_buckets(&self) -> Result<(), FullTextBitmapError> {
        let mut keys: Vec<u32> = self.token_deltas.iter().map(|e| *e.key()).collect();
        keys.sort_unstable();

        // Preload blocks for all delta keys so point reads hit cache.
        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(keys.iter().map(|k| (String::new(), *k)))
                .await;
        }

        for key in keys {
            let Some((_, delta)) = self.token_deltas.remove(&key) else {
                continue;
            };
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_bucket(key).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &delta.adds;
            bitmap -= &delta.deletes;

            if bitmap.is_empty() {
                self.bitmap_writer
                    .delete::<u32, RoaringBitmap>("", key)
                    .await?;
            } else {
                self.bitmap_writer.set("", key, bitmap).await?;
            }
        }
        Ok(())
    }

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
                    Some(r) => r.get_trigram(&trigram, key).await?,
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

    async fn write_transitions(&self) -> Result<(), FullTextBitmapError> {
        let mut keys: Vec<u32> = self.transition_deltas.iter().map(|e| *e.key()).collect();
        keys.sort_unstable();

        // Preload blocks for all transition keys (doc + bucket).
        if let Some(reader) = &self.old_reader {
            reader
                .load_keys(keys.iter().flat_map(|h| {
                    [
                        (String::new(), h | TRANSITION_DOC_FLAG),
                        (String::new(), h | TRANSITION_BUCKET_FLAG),
                    ]
                }))
                .await;
        }

        // Doc-ID bitmaps first: keys in [2^24, 2^25).
        for &hash in &keys {
            let Some(entry) = self.transition_deltas.get(&hash) else {
                continue;
            };
            if entry.doc_adds.is_empty() && entry.doc_deletes.is_empty() {
                continue;
            }
            let doc_key = hash | TRANSITION_DOC_FLAG;
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_bucket(doc_key).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &entry.doc_adds;
            bitmap -= &entry.doc_deletes;
            if !bitmap.is_empty() {
                self.bitmap_writer.set("", doc_key, bitmap).await?;
            }
        }

        // Bucket-ID bitmaps second: keys in [2^25, 2^26).
        for hash in keys {
            let Some((_, delta)) = self.transition_deltas.remove(&hash) else {
                continue;
            };
            if delta.bucket_adds.is_empty() {
                continue;
            }
            let bkt_key = hash | TRANSITION_BUCKET_FLAG;
            let mut bitmap = match &self.old_reader {
                Some(r) => r.get_bucket(bkt_key).await?,
                None => RoaringBitmap::new(),
            };
            bitmap |= &delta.bucket_adds;
            self.bitmap_writer.set("", bkt_key, bitmap).await?;
        }
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
// Reader (minimal — fork support only, search added in PR 3)
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

    /// Preload blocks for a set of `(prefix, key)` pairs into the cache.
    pub async fn load_keys(&self, keys: impl IntoIterator<Item = (String, u32)>) {
        self.bitmap_reader.load_data_for_keys(keys).await;
    }

    /// Load a bitmap by key under `prefix=""`.
    pub async fn get_bucket(&self, key: u32) -> Result<RoaringBitmap, FullTextBitmapError> {
        Ok(self.bitmap_reader.get("", key).await?.unwrap_or_default())
    }

    /// Load a trigram bitmap by trigram string and positional key.
    pub async fn get_trigram(
        &self,
        trigram: &str,
        key: u32,
    ) -> Result<RoaringBitmap, FullTextBitmapError> {
        Ok(self
            .bitmap_reader
            .get(trigram, key)
            .await?
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
        let bm = reader.get_bucket(hello_bucket).await.unwrap();
        assert!(!bm.contains(1));
        assert!(bm.contains(2));

        // "world" bucket (unique to doc 1): empty after delete.
        let world_bucket = *hw_buckets.iter().find(|b| !hr_buckets.contains(b)).unwrap();
        let bm = reader.get_bucket(world_bucket).await.unwrap();
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
        assert!(reader.get_bucket(hello).await.unwrap().contains(1));
        // "world" only in old — deleted.
        let world = *old_buckets
            .iter()
            .find(|b| !new_buckets.contains(b))
            .unwrap();
        assert!(!reader.get_bucket(world).await.unwrap().contains(1));
        // "rust" only in new — added.
        let rust = *new_buckets
            .iter()
            .find(|b| !old_buckets.contains(b))
            .unwrap();
        assert!(reader.get_bucket(rust).await.unwrap().contains(1));
    }

    #[tokio::test]
    async fn test_trigram_entries() {
        let (_tmp, provider) = provider();
        let writer = new_writer(&provider).await;
        let bucket = tokenize("hello").buckets[0];

        writer.add_document(1, tokenize("hello"));
        let reader = flush_and_read(writer, &provider).await;

        // "hel"(key=0), "ell"(key=1), "llo"(key=2)
        assert!(reader.get_trigram("hel", 0).await.unwrap().contains(bucket));
        assert!(reader.get_trigram("ell", 1).await.unwrap().contains(bucket));
        assert!(reader.get_trigram("llo", 2).await.unwrap().contains(bucket));
        // Wrong keys should not contain the bucket.
        assert!(!reader.get_trigram("hel", 1).await.unwrap().contains(bucket));
        assert!(!reader.get_trigram("llo", 0).await.unwrap().contains(bucket));
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
            let doc_bm = reader.get_bucket(hash | TRANSITION_DOC_FLAG).await.unwrap();
            assert!(doc_bm.contains(1));
            let bkt_bm = reader
                .get_bucket(hash | TRANSITION_BUCKET_FLAG)
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
            let doc_bm = reader.get_bucket(hash | TRANSITION_DOC_FLAG).await.unwrap();
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
        let bm = reader2.get_bucket(hello).await.unwrap();
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
        assert!(bm.contains(3));
    }
}
