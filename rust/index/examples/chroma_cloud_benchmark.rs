//! Benchmark BlockMaxMaxScore using sparse vectors from a Chroma Cloud collection.
//!
//! Fetches sparse vectors in parallel from any Chroma Cloud collection,
//! builds a BlockMaxMaxScore index, and benchmarks query performance.
//!
//! ## Usage
//!
//! ```bash
//! cargo run --release --example chroma_cloud_benchmark -- \
//!   --api-key "ck-..." \
//!   --database "Staging" \
//!   --collection "v1_3_broker_research" \
//!   --sparse-key "sparse_embedding" \
//!   -n 100000 -m 100 -k 10
//! ```
//!
//! The `--api-key` flag can also be set via the `CHROMA_API_KEY` env var.

use chroma::{ChromaHttpClient, ChromaHttpClientOptions};
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::maxscore::{
    rescore_and_select, BlockSparseReader, BlockSparseWriter, SparsePostingBlock, SparseRescorer,
    SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use chroma_types::operator::Key;
use chroma_types::plan::SearchPayload;
use chroma_types::{MetadataValue, SignedRoaringBitmap};
use clap::Parser;
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BLOCK_MAXSCORE_PREFIX: &str = "block_maxscore";

#[derive(Parser)]
#[command(about = "Benchmark BlockMaxMaxScore with Chroma Cloud sparse vectors")]
struct Args {
    /// Chroma Cloud API key (falls back to CHROMA_API_KEY env var)
    #[arg(long)]
    api_key: Option<String>,

    /// Chroma Cloud database name
    #[arg(long, default_value = "Staging")]
    database: String,

    /// Collection name to pull sparse vectors from
    #[arg(long)]
    collection: String,

    /// Metadata key containing the sparse vector
    #[arg(long, default_value = "sparse_embedding")]
    sparse_key: String,

    /// Number of documents to load (0 = all)
    #[arg(short = 'n', long, default_value_t = 0)]
    num_documents: usize,

    /// Number of queries (sampled from documents)
    #[arg(short = 'm', long, default_value_t = 100)]
    num_queries: usize,

    /// Top-k results
    #[arg(short = 'k', long, default_value_t = 10)]
    top_k: usize,

    /// Records per GET request
    #[arg(long, default_value_t = 300)]
    page_size: u32,

    /// Parallel fetch concurrency
    #[arg(long, default_value_t = 10)]
    concurrency: usize,

    /// Indexing batch size (docs per commit/flush)
    #[arg(long, default_value_t = 2000000)]
    batch_size: usize,

    /// Posting block size (entries per block)
    #[arg(long, default_value_t = 1024)]
    ms_block_size: u32,

    /// Max query terms: keep only the top-weighted terms per query.
    /// Documents have 100-300+ terms; real queries are ~5-30.
    /// Defaults to 24 to simulate realistic query workloads.
    #[arg(long, default_value_t = 24)]
    max_terms: usize,

    /// Skip brute force, run only MaxScore (profiling mode)
    #[arg(long)]
    maxscore_only: bool,

    /// Diagnose recall: run each query at k and 10×k, distinguish
    /// pruning bugs from quantization noise.
    #[arg(long)]
    diagnose: bool,

    /// Oversample factor for two-phase re-scoring. MaxScore returns
    /// k * oversample candidates, then exact re-scoring selects top-k.
    /// 0 = disabled (use raw MaxScore scores). Default 3.
    #[arg(long, default_value_t = 3)]
    oversample: u32,

    /// Iterations per query (for profiling)
    #[arg(short = 'i', long, default_value_t = 1)]
    iterations: usize,
}

impl Args {
    fn resolve_api_key(&self) -> anyhow::Result<String> {
        if let Some(key) = &self.api_key {
            return Ok(key.clone());
        }
        std::env::var("CHROMA_API_KEY")
            .map_err(|_| anyhow::anyhow!("Provide --api-key or set CHROMA_API_KEY env var"))
    }
}

#[derive(Clone)]
struct SparseDoc {
    indices: Vec<u32>,
    values: Vec<f32>,
}

// ── In-memory forward index for exact re-scoring ───────────────────

struct ForwardIndex {
    docs: HashMap<u32, Vec<(u32, f32)>>,
}

impl ForwardIndex {
    fn build(documents: &[SparseDoc]) -> Self {
        let mut docs = HashMap::with_capacity(documents.len());
        for (i, doc) in documents.iter().enumerate() {
            let mut pairs: Vec<(u32, f32)> = doc
                .indices
                .iter()
                .zip(doc.values.iter())
                .map(|(&d, &v)| (d, v))
                .collect();
            pairs.sort_unstable_by_key(|&(d, _)| d);
            docs.insert(i as u32, pairs);
        }
        ForwardIndex { docs }
    }

    fn dot_product(&self, doc_id: u32, query: &[(u32, f32)]) -> f32 {
        let Some(doc) = self.docs.get(&doc_id) else {
            return 0.0;
        };
        let mut score = 0.0f32;
        let mut di = 0;
        let mut qi = 0;
        while di < doc.len() && qi < query.len() {
            let dd = doc[di].0;
            let qd = query[qi].0;
            if dd < qd {
                di += 1;
            } else if dd > qd {
                qi += 1;
            } else {
                score += doc[di].1 * query[qi].1;
                di += 1;
                qi += 1;
            }
        }
        score
    }
}

#[async_trait::async_trait]
impl SparseRescorer for ForwardIndex {
    async fn rescore_batch(&self, doc_ids: &[u32], query: &[(u32, f32)]) -> Vec<f32> {
        doc_ids
            .iter()
            .map(|&id| self.dot_product(id, query))
            .collect()
    }
}

// ── Fetch documents from Chroma Cloud ──────────────────────────────

async fn fetch_documents(client: &ChromaHttpClient, args: &Args) -> anyhow::Result<Vec<SparseDoc>> {
    let collection = client
        .get_collection(&args.collection)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get collection '{}': {e}", args.collection))?;

    let total_count = collection
        .count()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to count: {e}"))? as usize;
    println!(
        "  Collection '{}' has {total_count} records",
        args.collection
    );

    let target = if args.num_documents == 0 {
        total_count
    } else {
        args.num_documents.min(total_count)
    };
    let page_size = args.page_size;
    let num_pages = target.div_ceil(page_size as usize);
    let sparse_key = args.sparse_key.clone();

    let pb = ProgressBar::new(target as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  Fetching [{bar:40}] {pos}/{len} records ({eta})")
            .unwrap(),
    );

    let collection = Arc::new(collection);

    // Use search() with select() to fetch ONLY the sparse vector
    // metadata key — avoids transferring all other metadata fields.
    let results: Vec<Result<Vec<SparseDoc>, anyhow::Error>> = stream::iter(0..num_pages)
        .map(|page_idx| {
            let collection = Arc::clone(&collection);
            let sparse_key = sparse_key.clone();
            let pb = pb.clone();
            async move {
                let offset = (page_idx as u32) * page_size;
                let limit = page_size.min((target - page_idx * page_size as usize) as u32);

                let payload = SearchPayload::default()
                    .limit(Some(limit), offset)
                    .select([Key::field(sparse_key.clone())]);

                let response = collection
                    .search(vec![payload])
                    .await
                    .map_err(|e| anyhow::anyhow!("search offset={offset}: {e}"))?;

                // search returns Vec-per-payload; we sent one payload.
                let metadatas = response
                    .metadatas
                    .into_iter()
                    .next()
                    .flatten()
                    .unwrap_or_default();

                let mut page_docs = Vec::with_capacity(metadatas.len());
                for meta_opt in metadatas {
                    let Some(meta) = meta_opt else { continue };
                    let Some(MetadataValue::SparseVector(sv)) = meta.get(&sparse_key) else {
                        continue;
                    };
                    page_docs.push(SparseDoc {
                        indices: sv.indices.clone(),
                        values: sv.values.clone(),
                    });
                }

                pb.inc(page_docs.len() as u64);
                Ok(page_docs)
            }
        })
        .buffer_unordered(args.concurrency)
        .collect()
        .await;

    pb.finish_and_clear();

    let mut all_docs = Vec::with_capacity(target);
    for result in results {
        all_docs.extend(result?);
    }
    all_docs.truncate(target);
    Ok(all_docs)
}

// ── Build BlockMaxMaxScore index ───────────────────────────────────

async fn build_index(
    documents: &[SparseDoc],
    provider: &BlockfileProvider,
    batch_size: usize,
    ms_block_size: u32,
) -> anyhow::Result<(Uuid, usize)> {
    let total = documents.len();
    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("  Indexing [{bar:40}] {pos}/{len} docs ({eta})")
            .unwrap(),
    );

    let mut posting_id = Uuid::nil();
    let mut chunks = 0usize;

    for (chunk_idx, chunk) in documents.chunks(batch_size).enumerate() {
        let mut posting_options = BlockfileWriterOptions::new(BLOCK_MAXSCORE_PREFIX.to_string())
            .ordered_mutations()
            .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES);

        if chunk_idx > 0 {
            posting_options = posting_options.fork(posting_id);
        }

        let posting_writer = provider
            .write::<u32, SparsePostingBlock>(posting_options)
            .await
            .map_err(|e| anyhow::anyhow!("Writer create: {e}"))?;

        let old_reader = if chunk_idx > 0 {
            let r = provider
                .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
                    posting_id,
                    BLOCK_MAXSCORE_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Reader open: {e}"))?;
            Some(BlockSparseReader::new(r))
        } else {
            None
        };

        let writer =
            BlockSparseWriter::new(posting_writer, old_reader).with_block_size(ms_block_size);

        let base_offset = chunk_idx * batch_size;
        for (i, doc) in chunk.iter().enumerate() {
            let offset = (base_offset + i) as u32;
            let entries: Vec<(u32, f32)> = doc
                .indices
                .iter()
                .zip(doc.values.iter())
                .map(|(&idx, &val)| (idx, val))
                .collect();
            writer.set(offset, entries).await;
        }

        let flusher = writer
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Commit: {e}"))?;
        posting_id = flusher.id();
        flusher
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("Flush: {e}"))?;
        chunks += 1;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_and_clear();
    Ok((posting_id, chunks))
}

// ── Search ─────────────────────────────────────────────────────────

async fn search(
    provider: &BlockfileProvider,
    posting_id: Uuid,
    query: &[(u32, f32)],
    k: u32,
    oversample: u32,
    rescorer: Option<&dyn SparseRescorer>,
) -> anyhow::Result<(Vec<(u32, f32)>, f64)> {
    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
            posting_id,
            BLOCK_MAXSCORE_PREFIX.to_string(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Reader: {e}"))?;
    let reader = BlockSparseReader::new(posting_reader);

    let fetch_k = if rescorer.is_some() && oversample > 1 {
        k * oversample
    } else {
        k
    };

    let mask = SignedRoaringBitmap::full();
    let start = Instant::now();
    let candidates = reader
        .query(query.to_vec(), fetch_k, mask)
        .await
        .map_err(|e| anyhow::anyhow!("Query: {e}"))?;

    let results = if let Some(rescorer) = rescorer {
        rescore_and_select(candidates, k as usize, query, rescorer).await
    } else {
        let mut r = candidates;
        r.truncate(k as usize);
        r
    };
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let pairs: Vec<(u32, f32)> = results.iter().map(|s| (s.offset, s.score)).collect();
    Ok((pairs, elapsed_ms))
}

// ── Brute force for recall measurement ─────────────────────────────

fn brute_force_topk(documents: &[SparseDoc], query: &[(u32, f32)], k: usize) -> Vec<(u32, f32)> {
    let mut scores: Vec<(u32, f32)> = Vec::new();

    for (doc_idx, doc) in documents.iter().enumerate() {
        let mut score = 0.0f32;
        let mut di = 0;
        let mut qi = 0;
        while di < doc.indices.len() && qi < query.len() {
            let dd = doc.indices[di];
            let qd = query[qi].0;
            if dd < qd {
                di += 1;
            } else if dd > qd {
                qi += 1;
            } else {
                score += doc.values[di] * query[qi].1;
                di += 1;
                qi += 1;
            }
        }
        if score > 0.0 {
            scores.push((doc_idx as u32, score));
        }
    }

    scores.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    scores.truncate(k);
    scores
}

fn recall(results: &[(u32, f32)], expected: &[(u32, f32)]) -> f32 {
    if expected.is_empty() {
        return 1.0;
    }
    let result_ids: std::collections::HashSet<u32> = results.iter().map(|(id, _)| *id).collect();
    let expected_ids: std::collections::HashSet<u32> = expected.iter().map(|(id, _)| *id).collect();
    result_ids.intersection(&expected_ids).count() as f32 / expected.len() as f32
}

fn dir_size_bytes(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                total += dir_size_bytes(&entry.path());
            } else {
                total += meta.len();
            }
        }
    }
    total
}

// ── Main ───────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let api_key = args.resolve_api_key()?;

    println!("🚀 Chroma Cloud → BlockMaxMaxScore Benchmark");
    println!("{}", "=".repeat(60));

    println!(
        "\n📡 Connecting to Chroma Cloud (database: '{}')...",
        args.database
    );
    let client = ChromaHttpClient::new(ChromaHttpClientOptions::cloud(&api_key, &args.database)?);

    println!("📥 Fetching sparse vectors from '{}'...", args.collection);
    let start = Instant::now();
    let documents = fetch_documents(&client, &args).await?;
    let fetch_time = start.elapsed().as_secs_f64();
    let num_docs = documents.len();

    if num_docs == 0 {
        anyhow::bail!(
            "No documents with sparse vector key '{}' found",
            args.sparse_key
        );
    }

    let total_nnz: usize = documents.iter().map(|d| d.indices.len()).sum();
    let avg_nnz = total_nnz as f64 / num_docs as f64;
    let max_dim = documents
        .iter()
        .flat_map(|d| d.indices.iter())
        .max()
        .unwrap_or(&0);

    println!("✅ Loaded {num_docs} documents in {fetch_time:.1}s");
    println!("  Total non-zeros: {total_nnz}");
    println!("  Avg non-zeros/doc: {avg_nnz:.1}");
    println!("  Max dimension: {max_dim}");

    // Sample queries: evenly spaced docs, truncated to top-weighted terms
    println!(
        "\n🔍 Sampling {} queries from documents (max {} terms)...",
        args.num_queries, args.max_terms
    );
    let step = (num_docs / args.num_queries.max(1)).max(1);
    let queries: Vec<Vec<(u32, f32)>> = (0..args.num_queries)
        .map(|i| {
            let doc = &documents[(i * step) % num_docs];
            let mut pairs: Vec<(u32, f32)> = doc
                .indices
                .iter()
                .zip(doc.values.iter())
                .map(|(&idx, &val)| (idx, val))
                .collect();

            if pairs.len() > args.max_terms {
                // Keep the highest-weighted terms (most discriminative)
                pairs.sort_unstable_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
                pairs.truncate(args.max_terms);
                // Re-sort by dimension ID for merge-join in the search algorithm
                pairs.sort_unstable_by_key(|&(dim, _)| dim);
            }
            pairs
        })
        .collect();

    let query_nnz: Vec<usize> = queries.iter().map(|q| q.len()).collect();
    let avg_query_nnz = query_nnz.iter().sum::<usize>() as f64 / query_nnz.len().max(1) as f64;
    let min_query_nnz = query_nnz.iter().min().copied().unwrap_or(0);
    let max_query_nnz = query_nnz.iter().max().copied().unwrap_or(0);
    println!("  Query terms: avg={avg_query_nnz:.1}, min={min_query_nnz}, max={max_query_nnz}");

    // Build index
    println!("\n🏗️ Building BlockMaxMaxScore index...");
    let (temp_dir, provider) = test_arrow_blockfile_provider(SPARSE_POSTING_BLOCK_SIZE_BYTES);
    let start = Instant::now();
    let (posting_id, chunks) =
        build_index(&documents, &provider, args.batch_size, args.ms_block_size).await?;
    let build_time = start.elapsed().as_secs_f64();

    let storage_bytes = dir_size_bytes(temp_dir.path());
    println!("⏱️  Index build: {build_time:.2}s");
    println!("  Chunks: {chunks}");
    println!(
        "  Storage: {:.2} MB",
        storage_bytes as f64 / (1024.0 * 1024.0)
    );

    // Build in-memory forward index for exact re-scoring
    let fwd_index = if args.oversample > 1 && !args.maxscore_only {
        println!("\n📇 Building in-memory forward index for re-scoring (oversample={}x)...", args.oversample);
        let start = Instant::now();
        let fi = ForwardIndex::build(&documents);
        println!("  Built in {:.2}s", start.elapsed().as_secs_f64());
        Some(fi)
    } else {
        None
    };
    let rescorer: Option<&dyn SparseRescorer> = fwd_index.as_ref().map(|fi| fi as &dyn SparseRescorer);
    let oversample = args.oversample;

    // Benchmark
    if args.maxscore_only {
        println!("\n🎯 MaxScore-only mode (no brute force, no re-scoring)");

        for q in queries.iter().take(5) {
            let _ = search(&provider, posting_id, q, args.top_k as u32, 1, None).await?;
        }

        let start_total = Instant::now();
        let mut total_ms = 0.0f64;
        for q in &queries {
            for _ in 0..args.iterations {
                let (_, elapsed) = search(&provider, posting_id, q, args.top_k as u32, 1, None).await?;
                total_ms += elapsed;
            }
        }
        let wall_time = start_total.elapsed().as_secs_f64() * 1000.0;
        let total_searches = queries.len() * args.iterations;
        let avg_ms = total_ms / total_searches as f64;

        println!("\n📨 RESULTS");
        println!("{}", "=".repeat(60));
        println!("  Documents: {num_docs}");
        println!(
            "  Queries: {} × {} iter = {total_searches}",
            queries.len(),
            args.iterations
        );
        println!("  Avg latency: {avg_ms:.2} ms");
        println!("  Wall time: {wall_time:.0} ms");
        println!(
            "  Throughput: {:.0} qps",
            total_searches as f64 / (wall_time / 1000.0)
        );
    } else if args.diagnose {
        // ── Diagnostic mode ────────────────────────────────────────
        let k = args.top_k;
        let big_k = (k * 10).min(1000) as u32;

        println!("\n🩺 Diagnosis mode: k={k}, big_k={big_k}, oversample={oversample}x");
        println!("  For each query: brute-force top-{k} vs index top-{k} and top-{big_k}");
        if rescorer.is_some() {
            println!("  Re-scoring enabled via in-memory forward index");
        }

        let mut total_recall_k = 0.0f32;
        let mut total_recall_big = 0.0f32;
        let mut pruning_misses = 0usize;
        let mut quant_misses = 0usize;
        let mut total_misses = 0usize;

        let pb = ProgressBar::new(queries.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("  Diagnosing [{bar:40}] {pos}/{len}")
                .unwrap(),
        );

        for (qi, q) in queries.iter().enumerate() {
            let expected = brute_force_topk(&documents, q, k);
            let (results_k, _) = search(&provider, posting_id, q, k as u32, oversample, rescorer).await?;
            let (results_big, _) = search(&provider, posting_id, q, big_k, oversample, rescorer).await?;

            let expected_ids: std::collections::HashSet<u32> =
                expected.iter().map(|(id, _)| *id).collect();
            let result_k_ids: std::collections::HashSet<u32> =
                results_k.iter().map(|(id, _)| *id).collect();
            let result_big_ids: std::collections::HashSet<u32> =
                results_big.iter().map(|(id, _)| *id).collect();

            let recall_k = recall(&results_k, &expected);
            let recall_big = if expected.is_empty() {
                1.0
            } else {
                expected_ids.intersection(&result_big_ids).count() as f32
                    / expected.len() as f32
            };

            total_recall_k += recall_k;
            total_recall_big += recall_big;

            let missing_from_k: Vec<u32> = expected_ids
                .difference(&result_k_ids)
                .copied()
                .collect();

            for &doc_id in &missing_from_k {
                total_misses += 1;
                if result_big_ids.contains(&doc_id) {
                    quant_misses += 1;
                } else {
                    pruning_misses += 1;
                    if pruning_misses <= 10 {
                        let bf_score = expected.iter().find(|(id, _)| *id == doc_id)
                            .map(|(_, s)| *s).unwrap_or(0.0);
                        let idx_min = results_k.last().map(|(_, s)| *s).unwrap_or(0.0);
                        println!(
                            "  ⚠️ Q{qi} doc {doc_id}: bf_score={bf_score:.6}, \
                             idx_min_score={idx_min:.6}, NOT in top-{big_k} → pruning bug"
                        );
                    }
                }
            }
            pb.inc(1);
        }
        pb.finish_and_clear();

        let n = queries.len() as f32;
        println!("\n🩺 DIAGNOSIS RESULTS");
        println!("{}", "=".repeat(60));
        println!("  Oversample:        {oversample}x");
        println!("  Re-scoring:        {}", if rescorer.is_some() { "enabled" } else { "disabled" });
        println!("  Recall@{k}:       {:.2}%", total_recall_k / n * 100.0);
        println!("  Recall@{big_k} → @{k}: {:.2}%", total_recall_big / n * 100.0);
        println!();
        println!("  Total missing docs:           {total_misses}");
        println!(
            "  Due to quantization noise:    {quant_misses} (in top-{big_k} but not top-{k})"
        );
        println!(
            "  Due to pruning (bug):         {pruning_misses} (not even in top-{big_k})"
        );
        if pruning_misses > 0 {
            println!(
                "\n  ❌ {pruning_misses} docs were pruned that shouldn't have been."
            );
            println!("     This indicates a bug in the MaxScore pruning logic.");
        } else if total_misses == 0 {
            println!("\n  ✅ Perfect recall — no missing docs.");
        } else {
            println!(
                "\n  ✅ All missing docs are ranking noise (in top-{big_k} but not top-{k})."
            );
        }
    } else {
        println!("\n🎯 Benchmark with brute force comparison...");
        if rescorer.is_some() {
            println!("  Re-scoring enabled (oversample={oversample}x)");
        }

        let mut total_recall = 0.0f32;
        let mut total_bf_ms = 0.0f64;
        let mut total_ms_ms = 0.0f64;

        let pb = ProgressBar::new(queries.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("  Querying [{bar:40}] {pos}/{len}")
                .unwrap(),
        );

        for q in &queries {
            let bf_start = Instant::now();
            let expected = brute_force_topk(&documents, q, args.top_k);
            total_bf_ms += bf_start.elapsed().as_secs_f64() * 1000.0;

            let (results, elapsed) = search(&provider, posting_id, q, args.top_k as u32, oversample, rescorer).await?;
            total_ms_ms += elapsed;
            total_recall += recall(&results, &expected);
            pb.inc(1);
        }
        pb.finish_and_clear();

        let n = queries.len() as f64;
        let avg_bf = total_bf_ms / n;
        let avg_ms = total_ms_ms / n;
        let avg_recall = total_recall / n as f32;
        let speedup = avg_bf / avg_ms;

        println!("\n📨 BENCHMARK RESULTS");
        println!("{}", "=".repeat(60));
        println!("🎯 Performance:");
        println!("  Oversample:         {oversample}x{}", if rescorer.is_some() { " (re-scoring enabled)" } else { "" });
        println!("  Method              Time (ms)    Speedup");
        println!("  ------------------------------------------");
        println!("  Brute Force         {avg_bf:<12.2} 1.00x");
        println!("  BlockMaxMaxScore    {avg_ms:<12.2} {speedup:.2}x");
        println!();
        println!("🔍 Recall@{}: {:.2}%", args.top_k, avg_recall * 100.0);
        println!();
        println!("📊 Dataset:");
        println!("  Documents: {num_docs}");
        println!("  Queries: {}", queries.len());
        println!("  Avg nnz/doc: {avg_nnz:.1}");
        println!("  Avg query terms: {avg_query_nnz:.1}");
    }

    drop(temp_dir);
    println!("\n🎉 Done!");
    Ok(())
}
