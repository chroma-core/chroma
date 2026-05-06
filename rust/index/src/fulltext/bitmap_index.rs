use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use dashmap::DashMap;
use murmur3::murmur3_32;
use roaring::RoaringBitmap;
use thiserror::Error;
use uuid::Uuid;

use super::tokenizer::{QueryError, WordAnalyzer};

// TODO: Persist these parameters (hash seed, bits, algorithm version) in the
// blockfile or segment metadata for forward compatibility. Currently hardcoded.

/// Number of bits in the hash bucket index.
///
/// 20 bits = 1,048,576 buckets. With a typical vocabulary of ~5M words,
/// this gives ~5 words per bucket on average. The probability of a rare
/// word colliding with a top-10K common word is ~1%, and multi-word AND
/// queries recover selectivity (0.01^N for N terms). Storage is dominated
/// by common-word bitmaps regardless of bucket count, so increasing bits
/// adds mostly empty buckets with negligible cost.
const HASH_BITS: u32 = 20;
const NUM_BUCKETS: u32 = 1 << HASH_BITS;

/// Murmur3 seed for token hashing.
///
/// A non-default seed to avoid collision patterns with other murmur3 usage
/// in the codebase (e.g., BM25 sparse embeddings use seed 0). The specific
/// value is arbitrary but fixed — changing it would invalidate all existing
/// blockfiles.
const HASH_SEED: u32 = 0x5f3759df;

#[derive(Debug, Error)]
pub enum FullTextBitmapError {
    #[error("Blockfile error: {0}")]
    Blockfile(#[from] Box<dyn ChromaError>),
    #[error("Query error: {0}")]
    Query(#[from] QueryError),
}

impl ChromaError for FullTextBitmapError {
    fn code(&self) -> ErrorCodes {
        match self {
            FullTextBitmapError::Blockfile(e) => e.code(),
            FullTextBitmapError::Query(_) => ErrorCodes::InvalidArgument,
        }
    }
}

/// Hash a token string to a bucket index in [0, NUM_BUCKETS).
fn hash_token(token: &str) -> u32 {
    let hash = murmur3_32(&mut Cursor::new(token.as_bytes()), HASH_SEED)
        .expect("murmur3_32 should not fail on in-memory data");
    hash % NUM_BUCKETS
}

/// Per-bucket accumulator tracking doc IDs to add and remove.
///
/// When both `adds` and `deletes` contain the same doc ID for a bucket,
/// the add takes precedence (the document was updated and still has a
/// token hashing to this bucket).
#[derive(Default)]
struct BucketDelta {
    adds: RoaringBitmap,
    deletes: RoaringBitmap,
}

/// Writer for the word-based full-text bitmap index.
///
/// Tokenizes documents with [`WordAnalyzer`], hashes each token to one of
/// 2^20 buckets via murmur3, and stores a [`RoaringBitmap`] per bucket in
/// a blockfile.
///
/// Supports concurrent mutation via [`DashMap`]. Call [`add_document`] and
/// [`delete_document`] from multiple threads, then [`write_to_blockfiles`]
/// and [`commit`] once.
#[derive(Clone)]
pub struct FullTextBitmapWriter {
    analyzer: WordAnalyzer,
    delta: Arc<DashMap<u32, BucketDelta>>,
    bitmap_writer: BlockfileWriter,
    old_reader: Option<FullTextBitmapReader>,
}

impl FullTextBitmapWriter {
    pub fn new(
        bitmap_writer: BlockfileWriter,
        analyzer: WordAnalyzer,
        old_reader: Option<FullTextBitmapReader>,
    ) -> Self {
        Self {
            analyzer,
            delta: Arc::new(DashMap::new()),
            bitmap_writer,
            old_reader,
        }
    }

    /// Add a document to the index. Tokenizes the text and marks the
    /// doc ID for insertion into each token's hash bucket.
    ///
    /// Clears any pending delete for the same (bucket, doc_id) so the
    /// last operation wins.
    pub fn add_document(&self, offset_id: u32, text: &str) {
        let mut analyzer = self.analyzer.clone();
        for token in analyzer.tokenize(text) {
            let bucket = hash_token(&token);
            let mut entry = self.delta.entry(bucket).or_default();
            entry.deletes.remove(offset_id);
            entry.adds.insert(offset_id);
        }
    }

    /// Delete a document from the index. Tokenizes the text and marks the
    /// doc ID for removal from each token's hash bucket.
    ///
    /// Clears any pending add for the same (bucket, doc_id) so the
    /// last operation wins.
    pub fn delete_document(&self, offset_id: u32, text: &str) {
        let mut analyzer = self.analyzer.clone();
        for token in analyzer.tokenize(text) {
            let bucket = hash_token(&token);
            let mut entry = self.delta.entry(bucket).or_default();
            entry.adds.remove(offset_id);
            entry.deletes.insert(offset_id);
        }
    }

    /// Merge accumulated deltas into the blockfile.
    ///
    /// Writes buckets in sorted order (required by ordered blockfile writer).
    /// Adds and deletes are disjoint per (bucket, doc_id) — enforced by
    /// `add_document`/`delete_document` clearing the opposite set.
    ///
    /// When forking from an existing blockfile via `old_reader`, existing
    /// bitmaps are merged: `result = (existing | adds) - deletes`.
    pub async fn write_to_blockfiles(&self) -> Result<(), FullTextBitmapError> {
        let mut bucket_ids: Vec<u32> = self.delta.iter().map(|e| *e.key()).collect();
        bucket_ids.sort_unstable();

        for bucket_id in bucket_ids {
            let Some((_, delta)) = self.delta.remove(&bucket_id) else {
                continue;
            };

            let mut bitmap = match &self.old_reader {
                Some(reader) => reader.get_bucket(bucket_id).await?,
                None => RoaringBitmap::new(),
            };
            bitmap -= &delta.deletes;
            bitmap |= &delta.adds;

            if bitmap.is_empty() {
                self.bitmap_writer
                    .delete::<u32, RoaringBitmap>("", bucket_id)
                    .await?;
            } else {
                self.bitmap_writer.set("", bucket_id, bitmap).await?;
            }
        }
        Ok(())
    }

    /// Commit the blockfile, returning a flusher.
    pub async fn commit(self) -> Result<FullTextBitmapFlusher, FullTextBitmapError> {
        let flusher = self.bitmap_writer.commit::<u32, RoaringBitmap>().await?;
        Ok(FullTextBitmapFlusher { flusher })
    }
}

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

/// Reader for the word-based full-text bitmap index.
///
/// Wraps a blockfile containing `(bucket_id: u32) → RoaringBitmap` entries.
/// Queries are decomposed into tokens, hashed to bucket IDs, and the
/// corresponding bitmaps are AND'd together.
#[derive(Clone)]
pub struct FullTextBitmapReader {
    bitmap_reader: BlockfileReader<'static, u32, RoaringBitmap>,
    analyzer: WordAnalyzer,
}

impl FullTextBitmapReader {
    pub fn new(
        bitmap_reader: BlockfileReader<'static, u32, RoaringBitmap>,
        analyzer: WordAnalyzer,
    ) -> Self {
        Self {
            bitmap_reader,
            analyzer,
        }
    }

    /// Search for documents matching the query string.
    ///
    /// Tokenizes the query, hashes each full token to a bucket, loads the
    /// bitmaps, and AND's them together. Returns a `RoaringBitmap` of
    /// candidate document offset IDs.
    ///
    /// Only full tokens (middle `tokens` in `FullTextQuery`) are used for
    /// matching. Prefix and suffix are ignored for now.
    // TODO: prefix/suffix require dictionary-based partial matching (PR 3).
    pub async fn search(&self, query: &str) -> Result<RoaringBitmap, FullTextBitmapError> {
        let mut analyzer = self.analyzer.clone();
        let ftq = analyzer.tokenize_query(query)?;

        // Collect all tokens to look up. All are hashed to bucket IDs.
        // TODO: prefix and suffix are partial matches — they should use
        // dictionary-based lookup to find all words matching the partial
        // and OR their bitmaps (PR 3). For now, hash them directly which
        // only matches the exact token, not words containing it.
        let token_set: HashSet<&str> = ftq
            .prefix
            .iter()
            .chain(ftq.tokens.iter())
            .chain(ftq.suffix.iter())
            .map(|s| s.as_str())
            .collect();

        let mut result: Option<RoaringBitmap> = None;
        for token in token_set {
            let bitmap = self.get_bucket(hash_token(token)).await?;
            result = Some(match result {
                Some(r) => r & &bitmap,
                None => bitmap,
            });
        }

        Ok(result.unwrap_or_default())
    }

    /// The blockfile ID backing this reader.
    pub fn id(&self) -> Uuid {
        self.bitmap_reader.id()
    }

    /// Load a single bucket's bitmap. Returns empty if the bucket doesn't exist.
    async fn get_bucket(&self, bucket_id: u32) -> Result<RoaringBitmap, FullTextBitmapError> {
        Ok(self
            .bitmap_reader
            .get("", bucket_id)
            .await?
            .unwrap_or_default())
    }
}

#[cfg(test)]
#[allow(clippy::large_futures)]
mod tests {
    use super::*;
    use chroma_blockstore::{
        arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider,
        BlockfileWriterOptions,
    };

    async fn new_writer() -> (FullTextBitmapWriter, BlockfileProvider, String) {
        let provider = BlockfileProvider::new_memory();
        let prefix = String::from("");
        let writer = provider
            .write::<u32, RoaringBitmap>(BlockfileWriterOptions::new(prefix.clone()))
            .await
            .unwrap();
        let bm_writer = FullTextBitmapWriter::new(writer, WordAnalyzer::new(), None);
        (bm_writer, provider, prefix)
    }

    async fn commit_and_read(
        writer: FullTextBitmapWriter,
        provider: &BlockfileProvider,
        prefix: &str,
    ) -> FullTextBitmapReader {
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        let bitmap_reader = provider
            .read::<u32, RoaringBitmap>(BlockfileReaderOptions::new(id, prefix.to_string()))
            .await
            .unwrap();
        FullTextBitmapReader::new(bitmap_reader, WordAnalyzer::new())
    }

    async fn read_bucket(
        provider: &BlockfileProvider,
        id: Uuid,
        prefix: &str,
        bucket_id: u32,
    ) -> RoaringBitmap {
        let reader = provider
            .read::<u32, RoaringBitmap>(BlockfileReaderOptions::new(id, prefix.to_string()))
            .await
            .unwrap();
        reader
            .get("", bucket_id)
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_add_single_document() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        let bm = read_bucket(&provider, id, &prefix, hash_token("hello")).await;
        assert!(bm.contains(1));
        let bm = read_bucket(&provider, id, &prefix, hash_token("world")).await;
        assert!(bm.contains(1));
    }

    #[tokio::test]
    async fn test_add_multiple_documents() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        writer.add_document(2, "hello rust");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        let bm = read_bucket(&provider, id, &prefix, hash_token("hello")).await;
        assert!(bm.contains(1));
        assert!(bm.contains(2));

        let bm = read_bucket(&provider, id, &prefix, hash_token("world")).await;
        assert!(bm.contains(1));
        assert!(!bm.contains(2));
    }

    #[tokio::test]
    async fn test_delete_only() {
        // Deletion without a prior add in this batch — the doc existed in
        // a previous generation. Without a reader for the old blockfile,
        // only the delete is recorded. The effective bitmap is empty.
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(2, "hello rust");
        writer.delete_document(1, "hello world");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        let bm = read_bucket(&provider, id, &prefix, hash_token("hello")).await;
        assert!(!bm.contains(1));
        assert!(bm.contains(2));
    }

    #[tokio::test]
    async fn test_update_as_delete_then_add() {
        // Materialized update: delete old doc, add new doc (same offset_id).
        // Per doc, at most one operation: the caller materializes an update
        // as delete(old) + add(new). Adds take precedence for shared tokens.
        let (writer, provider, prefix) = new_writer().await;
        writer.delete_document(1, "hello world");
        writer.add_document(1, "hello rust");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        // "hello" in both old and new — add takes precedence.
        let bm = read_bucket(&provider, id, &prefix, hash_token("hello")).await;
        assert!(bm.contains(1));

        // "world" only in old — deleted.
        let bm = read_bucket(&provider, id, &prefix, hash_token("world")).await;
        assert!(!bm.contains(1));

        // "rust" only in new — added.
        let bm = read_bucket(&provider, id, &prefix, hash_token("rust")).await;
        assert!(bm.contains(1));
    }

    #[tokio::test]
    async fn test_short_tokens_ignored() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "a big house");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        let bm = read_bucket(&provider, id, &prefix, hash_token("big")).await;
        assert!(bm.contains(1));

        // "a" is 1 char, filtered by WordAnalyzer (min_length=2).
        let bm = read_bucket(&provider, id, &prefix, hash_token("a")).await;
        assert!(!bm.contains(1));
    }

    #[tokio::test]
    async fn test_unicode_normalization() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "café résumé");
        writer.write_to_blockfiles().await.unwrap();
        let flusher = writer.commit().await.unwrap();
        let id = flusher.id();
        flusher.flush().await.unwrap();

        // "café" normalizes to "cafe".
        let bm = read_bucket(&provider, id, &prefix, hash_token("cafe")).await;
        assert!(bm.contains(1));
    }

    // --- Reader / search tests ---

    #[tokio::test]
    async fn test_search_single_word() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        writer.add_document(2, "hello rust");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        let result = reader.search("hello").await.unwrap();
        assert!(result.contains(1));
        assert!(result.contains(2));
    }

    #[tokio::test]
    async fn test_search_multi_word() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        writer.add_document(2, "hello rust");
        writer.add_document(3, "world rust");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        // "hello world" → tokens ["hello", "world"], AND of their buckets.
        let result = reader.search("hello world").await.unwrap();
        assert!(result.contains(1));
        assert!(!result.contains(2)); // has "hello" but not "world"
        assert!(!result.contains(3)); // has "world" but not "hello"
    }

    #[tokio::test]
    async fn test_search_no_match() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        let result = reader.search("nonexistent").await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_search_unicode() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "café résumé");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        // Query "cafe" matches "café" after normalization.
        let result = reader.search("cafe").await.unwrap();
        assert!(result.contains(1));
    }

    #[tokio::test]
    async fn test_search_reject_short_query() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        // "a" is too short — no token survives the analyzer.
        assert!(reader.search("a").await.is_err());
    }

    #[tokio::test]
    async fn test_search_after_delete() {
        let (writer, provider, prefix) = new_writer().await;
        writer.add_document(1, "hello world");
        writer.add_document(2, "hello rust");
        writer.delete_document(1, "hello world");
        let reader = commit_and_read(writer, &provider, &prefix).await;

        let result = reader.search("hello").await.unwrap();
        assert!(!result.contains(1));
        assert!(result.contains(2));
    }

    #[tokio::test]
    async fn test_fork_with_old_reader() {
        use chroma_blockstore::arrow::config::BlockManagerConfig;
        use chroma_cache::new_cache_for_test;
        use chroma_storage::{local::LocalStorage, Storage};
        use tempfile::tempdir;

        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            new_cache_for_test(),
            new_cache_for_test(),
            BlockManagerConfig::default_num_concurrent_block_flushes(),
            BlockManagerConfig::default_max_concurrent_block_loads(),
        );
        let prefix = String::from("");

        // First generation: write docs 1 and 2.
        let writer1 = provider
            .write::<u32, RoaringBitmap>(BlockfileWriterOptions::new(prefix.clone()))
            .await
            .unwrap();
        let w1 = FullTextBitmapWriter::new(writer1, WordAnalyzer::new(), None);
        w1.add_document(1, "hello world");
        w1.add_document(2, "hello rust");
        let reader1 = commit_and_read(w1, &provider, &prefix).await;

        // Second generation: fork from reader, add doc 3, delete doc 1.
        let writer2 = provider
            .write::<u32, RoaringBitmap>(
                BlockfileWriterOptions::new(prefix.clone()).fork(reader1.id()),
            )
            .await
            .unwrap();
        let w2 = FullTextBitmapWriter::new(writer2, WordAnalyzer::new(), Some(reader1));
        w2.add_document(3, "hello chroma");
        w2.delete_document(1, "hello world");
        let reader2 = commit_and_read(w2, &provider, &prefix).await;

        let result = reader2.search("hello").await.unwrap();
        assert!(!result.contains(1)); // deleted
        assert!(result.contains(2)); // from first gen
        assert!(result.contains(3)); // added in second gen
    }
}
