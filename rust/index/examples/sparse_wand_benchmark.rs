//! Sparse Index Block-Max WAND Benchmark
//!
//! This benchmark evaluates the performance of the Block-Max WAND algorithm
//! for sparse vector search compared to brute force baseline.
//!
//! ## Usage Modes
//!
//! ### Full Benchmark Mode (default)
//! Compares WAND performance against brute force ground truth:
//! ```bash
//! cargo run --release --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
//!   -n 65536 \  # number of documents
//!   -m 200 \    # number of queries
//!   -k 128      # top-k results
//! ```
//!
//! ### With Filtering
//! Test WAND with a filter that excludes 30% of documents:
//! ```bash
//! cargo run --release --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
//!   -n 65536 \
//!   -m 200 \
//!   -k 128 \
//!   -f 30       # exclude 30% of documents
//! ```
//!
//! ### WAND-Only Mode (for profiling)
//! Runs only WAND without brute force comparison, useful for flamegraph profiling:
//! ```bash
//! cargo run --release --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
//!   --wand-only \
//!   -i 100  # run each query 100 times for better profiling
//! ```
//!
//! ### Flamegraph Profiling Example
//! ```bash
//! # Install flamegraph tools if needed
//! cargo install flamegraph
//!
//! # Run with profiling
//! cargo flamegraph --example sparse_wand_benchmark -- \
//!   -d /path/to/dataset \
//!   --wand-only \
//!   -n 10000 \
//!   -m 50 \
//!   -i 100
//! ```
//!
//! ## Options
//! - `--sort-by-url`: Sort documents by URL for better cache locality
//! - `--wand-only`: Skip brute force comparison for profiling
//! - `-i, --iterations`: Number of iterations per query (for profiling)
//! - `-f, --filter-percentage`: Randomly exclude a percentage of documents (0-100) to test filtering

use anyhow::Result;
use arrow::array::{Array, Float32Array, Int32Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::{
    reader::{Score, SparseReader},
    writer::SparseWriter,
};
use chroma_types::SignedRoaringBitmap;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sprs::CsVec;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::time::Instant;
use tempfile::TempDir;
use uuid::Uuid;

// Blockfile prefix constants
const SPARSE_MAX_PREFIX: &str = "sparse_max";
const SPARSE_OFFSET_VALUE_PREFIX: &str = "sparse_offset_value";

// Sparse document and query structures using CsVec from sprs
#[derive(Debug, Clone)]
pub struct SparseDocument {
    pub doc_id: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub sparse_vector: CsVec<f32>,
}

#[derive(Debug, Clone)]
pub struct SparseQuery {
    pub query_id: String,
    pub text: String,
    pub sparse_vector: CsVec<f32>,
}

/// Load documents with sparse vectors from parquet file
pub fn load_sparse_documents(
    path: String,
    offset: usize,
    limit: usize,
) -> Result<Vec<SparseDocument>> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut documents = Vec::new();
    let mut current_offset = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let batch_size = batch.num_rows();

        // Skip if we haven't reached the offset yet
        if current_offset + batch_size <= offset {
            current_offset += batch_size;
            continue;
        }

        // Process the batch
        let start = offset.saturating_sub(current_offset);
        let end = std::cmp::min(batch_size, start + (limit - documents.len()));

        if start < end {
            let sliced_batch = batch.slice(start, end - start);
            documents.extend(process_sparse_document_batch(sliced_batch)?);
        }

        current_offset += batch_size;

        // Stop if we've collected enough documents
        if documents.len() >= limit {
            break;
        }
    }

    Ok(documents)
}

/// Load queries with sparse vectors from parquet file
pub fn load_sparse_queries(path: String, offset: usize, limit: usize) -> Result<Vec<SparseQuery>> {
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let mut queries = Vec::new();
    let mut current_offset = 0;

    for batch_result in reader {
        let batch = batch_result?;
        let batch_size = batch.num_rows();

        // Skip if we haven't reached the offset yet
        if current_offset + batch_size <= offset {
            current_offset += batch_size;
            continue;
        }

        // Process the batch
        let start = offset.saturating_sub(current_offset);
        let end = std::cmp::min(batch_size, start + (limit - queries.len()));

        if start < end {
            let sliced_batch = batch.slice(start, end - start);
            queries.extend(process_sparse_query_batch(sliced_batch)?);
        }

        current_offset += batch_size;

        // Stop if we've collected enough queries
        if queries.len() >= limit {
            break;
        }
    }

    Ok(queries)
}

/// Process a document batch and extract sparse vectors
fn process_sparse_document_batch(batch: RecordBatch) -> Result<Vec<SparseDocument>> {
    let doc_ids = batch
        .column_by_name("doc_id")
        .ok_or_else(|| anyhow::anyhow!("doc_id column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("doc_id is not a string array"))?;

    let urls = batch
        .column_by_name("url")
        .ok_or_else(|| anyhow::anyhow!("url column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("url is not a string array"))?;

    let titles = batch
        .column_by_name("title")
        .ok_or_else(|| anyhow::anyhow!("title column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("title is not a string array"))?;

    let bodies = batch
        .column_by_name("body")
        .ok_or_else(|| anyhow::anyhow!("body column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("body is not a string array"))?;

    let sparse_indices = batch
        .column_by_name("sparse_indices")
        .ok_or_else(|| anyhow::anyhow!("sparse_indices column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_indices is not a list array"))?;

    let sparse_values = batch
        .column_by_name("sparse_values")
        .ok_or_else(|| anyhow::anyhow!("sparse_values column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_values is not a list array"))?;

    let mut documents = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let doc_id = doc_ids.value(i).to_string();
        let url = urls.value(i).to_string();
        let title = titles.value(i).to_string();
        let body = bodies.value(i).to_string();

        // Get indices and values for this document
        let indices_array = sparse_indices.value(i);
        let values_array = sparse_values.value(i);

        let indices = indices_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("indices is not Int32Array"))?;

        let values = values_array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("values is not Float32Array"))?;

        // Create sparse vector as CsVec
        let mut sparse_indices = Vec::new();
        let mut sparse_values = Vec::new();
        for j in 0..indices.len() {
            sparse_indices.push(indices.value(j) as usize);
            sparse_values.push(values.value(j));
        }
        let sparse_vector = CsVec::new(usize::MAX, sparse_indices, sparse_values);

        documents.push(SparseDocument {
            doc_id,
            url,
            title,
            body,
            sparse_vector,
        });
    }

    Ok(documents)
}

/// Process a query batch and extract sparse vectors
fn process_sparse_query_batch(batch: RecordBatch) -> Result<Vec<SparseQuery>> {
    let query_ids = batch
        .column_by_name("query_id")
        .ok_or_else(|| anyhow::anyhow!("query_id column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("query_id is not a string array"))?;

    let texts = batch
        .column_by_name("text")
        .ok_or_else(|| anyhow::anyhow!("text column not found"))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("text is not a string array"))?;

    let sparse_indices = batch
        .column_by_name("sparse_indices")
        .ok_or_else(|| anyhow::anyhow!("sparse_indices column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_indices is not a list array"))?;

    let sparse_values = batch
        .column_by_name("sparse_values")
        .ok_or_else(|| anyhow::anyhow!("sparse_values column not found"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow::anyhow!("sparse_values is not a list array"))?;

    let mut queries = Vec::with_capacity(batch.num_rows());

    for i in 0..batch.num_rows() {
        let query_id = query_ids.value(i).to_string();
        let text = texts.value(i).to_string();

        // Get indices and values for this query
        let indices_array = sparse_indices.value(i);
        let values_array = sparse_values.value(i);

        let indices = indices_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| anyhow::anyhow!("indices is not Int32Array"))?;

        let values = values_array
            .as_any()
            .downcast_ref::<Float32Array>()
            .ok_or_else(|| anyhow::anyhow!("values is not Float32Array"))?;

        // Create sparse vector as CsVec
        let mut sparse_indices = Vec::new();
        let mut sparse_values = Vec::new();
        for j in 0..indices.len() {
            sparse_indices.push(indices.value(j) as usize);
            sparse_values.push(values.value(j));
        }
        let sparse_vector = CsVec::new(usize::MAX, sparse_indices, sparse_values);

        queries.push(SparseQuery {
            query_id,
            text,
            sparse_vector,
        });
    }

    Ok(queries)
}

/// Command line arguments for the benchmark
#[derive(Parser, Debug)]
#[command(name = "sparse_wand_benchmark")]
#[command(about = "Benchmark sparse index with Block-Max WAND algorithm")]
struct Args {
    /// Path to the dataset directory containing documents.parquet and queries.parquet
    #[arg(short = 'd', long)]
    dataset_path: String,

    /// Number of documents to load
    #[arg(short = 'n', long, default_value_t = 65536)]
    num_documents: usize,

    /// Number of queries to run
    #[arg(short = 'm', long, default_value_t = 200)]
    num_queries: usize,

    /// Top-k results to retrieve
    #[arg(short = 'k', long, default_value_t = 128)]
    top_k: usize,

    /// Block size for the sparse index
    #[arg(short = 'b', long, default_value_t = 128)]
    block_size: u32,

    /// Sort documents by URL for better cache locality
    #[arg(short = 's', long)]
    sort_by_url: bool,

    /// Skip brute force comparison (WAND only mode for profiling)
    #[arg(long)]
    wand_only: bool,

    /// Number of iterations to run each query (for profiling)
    #[arg(short = 'i', long, default_value_t = 1)]
    iterations: usize,

    /// Filter percentage: randomly exclude this percentage of documents (0-100)
    #[arg(short = 'f', long, default_value_t = 0)]
    filter_percentage: u32,
}

#[derive(Debug, Clone)]
struct SearchResult {
    query_id: String,
    top_k_offsets: Vec<u32>,
    scores: Vec<f32>,
    search_time_ms: f64,
}

fn brute_force_search(
    documents: &[SparseDocument],
    query: &SparseQuery,
    top_k: usize,
    mask: &SignedRoaringBitmap,
) -> (SearchResult, usize) {
    let start = Instant::now();

    // Use a min-heap to maintain top-k results efficiently (same as WAND implementation)
    let mut top_scores = BinaryHeap::<Score>::with_capacity(top_k);
    let mut non_trivial_count = 0;

    for (offset, doc) in documents.iter().enumerate() {
        // Skip documents that are filtered out
        if !mask.contains(offset as u32) {
            continue;
        }

        // Use sprs dot product directly
        let score = query.sparse_vector.dot(&doc.sparse_vector);
        if score > 0.0 {
            non_trivial_count += 1;

            if top_scores.len() < top_k {
                top_scores.push(Score {
                    offset: offset as u32,
                    score,
                });
            } else if let Some(min_entry) = top_scores.peek() {
                if score > min_entry.score {
                    top_scores.pop();
                    top_scores.push(Score {
                        offset: offset as u32,
                        score,
                    });
                }
            }
        }
    }

    // Extract results from heap and sort by score descending
    let mut scores: Vec<Score> = top_scores.into_sorted_vec();
    scores.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap()
            .then(a.offset.cmp(&b.offset))
    });

    let elapsed = start.elapsed();

    let result = SearchResult {
        query_id: query.query_id.clone(),
        top_k_offsets: scores.iter().map(|s| s.offset).collect(),
        scores: scores.iter().map(|s| s.score).collect(),
        search_time_ms: elapsed.as_secs_f64() * 1000.0,
    };

    (result, non_trivial_count)
}

async fn build_sparse_index(
    documents: &[SparseDocument],
    block_size: u32,
    sort_by_url: bool,
) -> anyhow::Result<(TempDir, BlockfileProvider, Uuid, Uuid)> {
    println!("🏗️ Building sparse index...");
    let start = Instant::now();

    // Sort documents by URL if requested for better cache locality
    let mut sorted_documents = documents.to_vec();
    if sort_by_url {
        println!("🔗 Sorting documents by URL for better cache locality...");
        sorted_documents.sort_by(|a, b| a.url.cmp(&b.url));
    }

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    // Process documents in batches with write-commit-flush loop
    let batch_size = 65536;
    let num_chunks = sorted_documents.len().div_ceil(batch_size);

    let pb = ProgressBar::new(num_chunks as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} chunks ({eta})",
            )
            .unwrap()
            .progress_chars("█▉▊▋▌▍▎▏  "),
    );
    pb.set_message("Building index chunks");

    let mut max_writer_id = None;
    let mut offset_value_writer_id = None;

    for (chunk_idx, chunk) in sorted_documents.chunks(batch_size).enumerate() {
        // Create writer options, forking if not the first chunk
        let mut max_writer_options = BlockfileWriterOptions::new(SPARSE_MAX_PREFIX.to_string());
        let mut offset_value_writer_options =
            BlockfileWriterOptions::new(SPARSE_OFFSET_VALUE_PREFIX.to_string());

        if let Some(id) = max_writer_id {
            max_writer_options = max_writer_options.fork(id);
        }
        if let Some(id) = offset_value_writer_id {
            offset_value_writer_options = offset_value_writer_options.fork(id);
        }

        // Create writers for this chunk
        let max_writer = provider
            .write::<u32, f32>(max_writer_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create max writer: {:?}", e))?;

        let offset_value_writer = provider
            .write::<u32, f32>(offset_value_writer_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create offset value writer: {:?}", e))?;

        // Create reader for existing data (if we have previous committed data)
        let sparse_reader = if let (Some(max_id), Some(offset_value_id)) =
            (max_writer_id, offset_value_writer_id)
        {
            let max_reader = provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    max_id,
                    SPARSE_MAX_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create max reader: {:?}", e))?;

            let offset_value_reader = provider
                .read::<u32, f32>(BlockfileReaderOptions::new(
                    offset_value_id,
                    SPARSE_OFFSET_VALUE_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create offset value reader: {:?}", e))?;

            Some(SparseReader::new(max_reader, offset_value_reader))
        } else {
            None
        };

        // Create sparse writer for this chunk
        let sparse_writer = SparseWriter::new(
            block_size,
            max_writer.clone(),
            offset_value_writer.clone(),
            sparse_reader,
        );

        // Write documents in this chunk
        for (idx, doc) in chunk.iter().enumerate() {
            let offset = (chunk_idx * batch_size + idx) as u32;

            // Convert CsVec to iterator of (dimension_id, value)
            let sparse_iter = doc
                .sparse_vector
                .indices()
                .iter()
                .zip(doc.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val));
            sparse_writer.set(offset, sparse_iter).await;
        }

        // Commit
        let flusher = Box::pin(sparse_writer.commit())
            .await
            .map_err(|e| anyhow::anyhow!("Failed to commit sparse writer: {:?}", e))?;

        // Flush
        flusher
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to flush sparse writer: {:?}", e))?;

        // Store the writer IDs for forking in next iteration (after commit/flush)
        max_writer_id = Some(max_writer.id());
        offset_value_writer_id = Some(offset_value_writer.id());

        // Clear cache after each chunk
        provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clear cache: {:?}", e))?;

        pb.inc(1);
    }

    pb.finish_with_message("✅ Index built");

    let elapsed = start.elapsed();
    println!("⏱️ Index build time: {:.2} s", elapsed.as_secs_f64());
    println!("  Documents indexed: {}", sorted_documents.len());
    println!("  Chunks processed: {num_chunks}");
    println!("  Documents per chunk: {batch_size}");

    Ok((
        temp_dir,
        provider,
        max_writer_id.expect("Should have created at least one max writer"),
        offset_value_writer_id.expect("Should have created at least one offset value writer"),
    ))
}

#[allow(clippy::too_many_arguments)]
async fn search_with_wand(
    provider: &BlockfileProvider,
    max_reader_id: Uuid,
    offset_value_reader_id: Uuid,
    queries: &[SparseQuery],
    top_k: usize,
    mask: SignedRoaringBitmap,
    iterations: usize,
    show_progress: bool,
) -> anyhow::Result<Vec<SearchResult>> {
    if show_progress {
        println!("\n⚡ Searching with Block-Max WAND...");
    }

    // Open readers for the sparse index using the writer IDs
    let max_reader = provider
        .read::<u32, f32>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                max_reader_id,
                SPARSE_MAX_PREFIX.to_string(),
            ),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open max reader: {:?}", e))?;

    let offset_value_reader = provider
        .read::<u32, f32>(
            chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
                offset_value_reader_id,
                SPARSE_OFFSET_VALUE_PREFIX.to_string(),
            ),
        )
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open offset value reader: {:?}", e))?;

    let sparse_reader = SparseReader::new(max_reader, offset_value_reader);

    let mut results = Vec::new();

    let pb = if show_progress {
        let pb = ProgressBar::new((queries.len() * iterations) as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb.set_message(if iterations > 1 {
            format!("WAND search ({iterations} iterations)")
        } else {
            "WAND search".to_string()
        });
        Some(pb)
    } else {
        None
    };

    for query in queries {
        let mut total_time_ms = 0.0;
        let mut last_scores = Vec::new();
        let mut last_offsets = Vec::new();

        for _ in 0..iterations {
            let start = Instant::now();

            // Convert CsVec to Vec of (dimension_id, value)
            let query_vec: Vec<(u32, f32)> = query
                .sparse_vector
                .indices()
                .iter()
                .zip(query.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val))
                .collect();

            // Run WAND search with the provided mask
            let scores = sparse_reader
                .wand(query_vec, top_k as u32, mask.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Failed to run WAND search: {:?}", e))?;

            let elapsed = start.elapsed();
            total_time_ms += elapsed.as_secs_f64() * 1000.0;

            // Store results from last iteration
            last_offsets = scores.iter().map(|s| s.offset).collect();
            last_scores = scores.iter().map(|s| s.score).collect();

            if let Some(ref pb) = pb {
                pb.inc(1);
            }
        }

        results.push(SearchResult {
            query_id: query.query_id.clone(),
            top_k_offsets: last_offsets,
            scores: last_scores,
            search_time_ms: total_time_ms / iterations as f64, // Average time per query
        });
    }

    if let Some(pb) = pb {
        pb.finish_with_message("✅ WAND search complete");
    }
    Ok(results)
}

fn verify_and_compute_recall(
    documents: &[SparseDocument],
    queries: &[SparseQuery],
    reference: &[SearchResult],
    results: &[SearchResult],
) -> anyhow::Result<f64> {
    println!("\n🔍 Verifying WAND results and computing recall...");

    if reference.is_empty() {
        return Ok(if results.is_empty() { 1.0 } else { 0.0 });
    }

    let score_tolerance = 1e-5;
    let mut total_recall = 0.0;
    let mut count = 0;
    let mut queries_with_issues = 0;
    let mut verification_passed = true;

    for ref_result in reference {
        let query = queries
            .iter()
            .find(|q| q.query_id == ref_result.query_id)
            .ok_or_else(|| anyhow::anyhow!("Query {} not found", ref_result.query_id))?;

        if let Some(wand_result) = results.iter().find(|r| r.query_id == ref_result.query_id) {
            let mut query_has_issues = false;

            // 1. Check for duplicate documents in WAND results
            let unique_offsets: HashSet<u32> = wand_result.top_k_offsets.iter().cloned().collect();
            if unique_offsets.len() != wand_result.top_k_offsets.len() {
                println!(
                    "\n  ⚠️ Query {}: WAND returned {} documents but only {} are unique",
                    query.query_id,
                    wand_result.top_k_offsets.len(),
                    unique_offsets.len()
                );
                query_has_issues = true;
                verification_passed = false;
            }

            // 2. Verify scores by recomputing with brute force
            let mut score_errors = 0;
            for (&offset, &wand_score) in wand_result
                .top_k_offsets
                .iter()
                .zip(wand_result.scores.iter())
            {
                let doc = &documents[offset as usize];
                let actual_score = query.sparse_vector.dot(&doc.sparse_vector);

                let score_diff = (actual_score - wand_score).abs();
                if score_diff > score_tolerance {
                    if score_errors == 0 {
                        println!(
                            "\n  ⚠️ Query {} has score verification errors:",
                            query.query_id
                        );
                    }
                    if score_errors < 5 {
                        println!("     Doc {offset}: WAND={wand_score:.6}, Actual={actual_score:.6}, Diff={score_diff:.2e}");
                    }
                    score_errors += 1;
                    query_has_issues = true;
                    verification_passed = false;
                }
            }
            if score_errors > 5 {
                println!("     ... and {} more score mismatches", score_errors - 5);
            }

            // 3. Check recall considering tie-breaking
            let reference_ids: HashSet<_> = ref_result.top_k_offsets.iter().collect();
            let results_ids: HashSet<_> = wand_result.top_k_offsets.iter().collect();
            let found_docs = reference_ids.intersection(&results_ids).count();

            // Get minimum scores for tie-breaking analysis
            let ref_min_score = ref_result.scores.last().copied().unwrap_or(0.0);
            let wand_min_score = wand_result.scores.last().copied().unwrap_or(0.0);
            let min_score_diff = (ref_min_score - wand_min_score).abs();

            // Check if missing documents are due to tie-breaking
            let missing_docs: Vec<_> = reference_ids.difference(&results_ids).cloned().collect();
            let extra_docs: Vec<_> = results_ids.difference(&reference_ids).cloned().collect();

            let mut missing_due_to_ties = true;
            if !missing_docs.is_empty() {
                // Check if all missing docs have scores equal to the minimum (tie-breaking)
                for &missing_offset in &missing_docs {
                    let doc = &documents[*missing_offset as usize];
                    let missing_score = query.sparse_vector.dot(&doc.sparse_vector);
                    if (missing_score - ref_min_score).abs() > score_tolerance {
                        missing_due_to_ties = false;
                        break;
                    }
                }

                // Also verify that extra docs have similar scores (tie-breaking)
                for &extra_offset in &extra_docs {
                    let doc = &documents[*extra_offset as usize];
                    let extra_score = query.sparse_vector.dot(&doc.sparse_vector);
                    if (extra_score - wand_min_score).abs() > score_tolerance {
                        missing_due_to_ties = false;
                        break;
                    }
                }
            }

            // Calculate recall
            let recall = if ref_result.top_k_offsets.is_empty() {
                1.0
            } else if missing_due_to_ties && min_score_diff < score_tolerance {
                // If differences are only due to tie-breaking, consider it 100% recall
                1.0
            } else {
                found_docs as f64 / ref_result.top_k_offsets.len() as f64
            };

            // Report issues if recall is not 100% (excluding tie-breaking)
            if recall < 1.0 {
                println!(
                    "\n  ⚠️ Query {} has recall {:.2}% (not due to tie-breaking)",
                    ref_result.query_id,
                    recall * 100.0
                );
                println!(
                    "     Reference: {} docs, min score: {:.6}",
                    ref_result.top_k_offsets.len(),
                    ref_min_score
                );
                println!(
                    "     WAND: {} docs, min score: {:.6}",
                    wand_result.top_k_offsets.len(),
                    wand_min_score
                );
                println!(
                    "     Missing {} docs: {:?}",
                    missing_docs.len(),
                    missing_docs.iter().take(10).collect::<Vec<_>>()
                );
                query_has_issues = true;
                verification_passed = false;
            } else if !missing_docs.is_empty() {
                // Recall is 100% but there were tie-breaking differences
                println!("\n  ℹ️ Query {}: 100% recall (with {} tie-breaking differences at score {:.6})",
                    query.query_id, missing_docs.len(), ref_min_score
                );
            }

            if query_has_issues {
                queries_with_issues += 1;
            }

            total_recall += recall;
            count += 1;
        }
    }

    let avg_recall = if count > 0 {
        total_recall / count as f64
    } else {
        0.0
    };

    // Summary
    if verification_passed {
        println!("\n  ✅ All verifications passed!");
        println!("     - No duplicate documents");
        println!("     - All scores match actual computation (tolerance: {score_tolerance:.2e})");
        println!(
            "     - Recall: {:.2}% (tie-breaking handled correctly)",
            avg_recall * 100.0
        );
    } else {
        println!("\n  ⚠️ Verification found issues in {queries_with_issues}/{count} queries");
        println!("     - Average recall: {:.2}%", avg_recall * 100.0);
    }

    Ok(avg_recall)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments using clap
    let args = Args::parse();

    // Construct paths to documents and queries files
    let documents_path = format!("{}/documents.parquet", args.dataset_path);
    let queries_path = format!("{}/queries.parquet", args.dataset_path);

    println!("🚀 Sparse Index Block-Max WAND Benchmark");
    println!("{}", "=".repeat(60));
    println!("Configuration:");
    println!("  Dataset: {}", args.dataset_path);
    println!("  Documents: {documents_path}");
    println!("  Queries: {queries_path}");
    println!("  Num documents: {}", args.num_documents);
    println!("  Num queries: {}", args.num_queries);
    println!("  Top-k: {}", args.top_k);
    println!("  Block size: {}", args.block_size);
    println!("  Sort by URL: {}", args.sort_by_url);
    if args.filter_percentage > 0 {
        println!(
            "  Filter: {}% of documents excluded",
            args.filter_percentage
        );
    }
    println!(
        "  Mode: {}",
        if args.wand_only {
            "WAND only (profiling)"
        } else {
            "Full benchmark"
        }
    );
    if args.iterations > 1 {
        println!("  Iterations per query: {}", args.iterations);
    }
    println!();

    // Load documents
    println!("📄 Loading documents...");
    let documents = load_sparse_documents(documents_path, 0, args.num_documents)?;
    println!("✅ Loaded {} documents", documents.len());

    // Load queries
    println!("🔍 Loading queries...");
    let queries = load_sparse_queries(queries_path, 0, args.num_queries)?;
    println!("✅ Loaded {} queries", queries.len());

    // Build sparse index
    let (temp_dir, provider, max_reader_id, offset_value_reader_id) = Box::pin(build_sparse_index(
        &documents,
        args.block_size,
        args.sort_by_url,
    ))
    .await?;

    // Create mask based on filter percentage
    let mask = if args.filter_percentage > 0 {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut excluded = roaring::RoaringBitmap::new();
        for i in 0..documents.len() as u32 {
            if rng.gen_range(0..100) < args.filter_percentage {
                excluded.insert(i);
            }
        }
        println!(
            "🎭 Filter mask created: {} documents excluded",
            excluded.len()
        );
        SignedRoaringBitmap::Exclude(excluded)
    } else {
        SignedRoaringBitmap::full()
    };

    if args.wand_only {
        // WAND-only mode for profiling
        println!("\n🎯 Running WAND-only mode (no brute force comparison)");
        if args.iterations > 1 {
            println!(
                "Running {} iterations per query for profiling...",
                args.iterations
            );
        }

        let start_total = Instant::now();
        let wand_results = search_with_wand(
            &provider,
            max_reader_id,
            offset_value_reader_id,
            &queries,
            args.top_k,
            mask,
            args.iterations,
            false, // no progress bar in wand-only mode
        )
        .await?;
        let total_time = start_total.elapsed().as_secs_f64() * 1000.0;

        // Calculate metrics
        let avg_wand_time =
            wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

        let total_iterations = queries.len() * args.iterations;

        // Print results
        println!("\n📨 WAND-ONLY RESULTS");
        println!("{}", "=".repeat(60));
        println!("🎯 Performance:");
        println!("  Total time: {total_time:.2} ms");
        println!("  Total iterations: {total_iterations}");
        println!("  Avg time per query: {avg_wand_time:.2} ms");
        println!("  Queries per second: {:.2}", 1000.0 / avg_wand_time);

        println!();
        println!("📊 Statistics:");
        println!("  Documents indexed: {}", documents.len());
        println!("  Queries processed: {}", queries.len());
        println!("  Iterations per query: {}", args.iterations);

        println!("\n🔥 Ready for flamegraph profiling!");
        println!("Tip: Use with cargo flamegraph --freq 99 for lower overhead");
    } else {
        // Full benchmark mode with brute force comparison
        println!("\n🐌 Running brute force search (ground truth)...");
        let pb_brute = ProgressBar::new(queries.len() as u64);
        pb_brute.set_style(
            ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.yellow/blue}] {pos}/{len} ({eta})",
                )
                .unwrap()
                .progress_chars("█▉▊▋▌▍▎▏  "),
        );
        pb_brute.set_message("Brute force search");

        let start = Instant::now();
        let mut brute_force_results = Vec::new();
        let mut total_non_trivial = 0;

        for (i, query) in queries.iter().enumerate() {
            let (result, non_trivial_count) =
                brute_force_search(&documents, query, args.top_k, &mask);
            total_non_trivial += non_trivial_count;
            brute_force_results.push(result);
            pb_brute.set_position((i + 1) as u64);
        }

        pb_brute.finish_with_message("✅ Brute force complete");
        let brute_force_time = start.elapsed().as_secs_f64() * 1000.0;

        // Calculate non-trivial document statistics
        let avg_non_trivial = total_non_trivial as f64 / queries.len() as f64;
        let avg_percentage = (avg_non_trivial / documents.len() as f64) * 100.0;

        println!("Brute force total time: {brute_force_time:.2} ms");
        println!(
            "Average documents with non-zero similarity: {:.1}/{} ({:.1}%)",
            avg_non_trivial,
            documents.len(),
            avg_percentage
        );

        // Run WAND search
        let wand_results = search_with_wand(
            &provider,
            max_reader_id,
            offset_value_reader_id,
            &queries,
            args.top_k,
            mask.clone(),
            args.iterations,
            true, // show progress in full benchmark mode
        )
        .await?;

        // Verify WAND results and compute recall
        let recall =
            verify_and_compute_recall(&documents, &queries, &brute_force_results, &wand_results)?;

        let avg_brute_force_time = brute_force_results
            .iter()
            .map(|r| r.search_time_ms)
            .sum::<f64>()
            / brute_force_results.len() as f64;

        let avg_wand_time =
            wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

        let speedup = avg_brute_force_time / avg_wand_time;

        // Print results
        println!("\n📨 BENCHMARK RESULTS");
        println!("{}", "=".repeat(60));
        println!("🎯 Performance Comparison:");
        println!("  Method              Time (ms)    Speedup");
        println!("  {}", "-".repeat(42));
        println!("  Brute Force         {avg_brute_force_time:<12.2} 1.00x");
        println!("  Block-Max WAND      {avg_wand_time:<12.2} {speedup:.2}x");
        if args.iterations > 1 {
            println!("  (WAND averaged over {} iterations)", args.iterations);
        }
        println!();
        println!("🔍 Quality Metrics:");
        println!("  Recall@{}: {:.2}%", args.top_k, recall * 100.0);
        println!();
        println!("📊 Dataset Statistics:");
        println!("  Documents processed: {}", documents.len());
        println!("  Queries processed: {}", queries.len());
        println!("  Avg non-zero docs per query: {avg_non_trivial:.1} ({avg_percentage:.1}%)");

        println!("\n🎉 Benchmark completed successfully!");
        println!("Total queries processed: {}", queries.len());
        if args.sort_by_url {
            println!("Documents were sorted by URL for better cache locality");
        }
    }

    // Clean up
    drop(provider);
    drop(temp_dir);

    Ok(())
}
