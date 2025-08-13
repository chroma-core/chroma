use anyhow::Result;
use arrow::array::{Array, Float32Array, Int32Array, ListArray, StringArray};
use arrow::record_batch::RecordBatch;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::{
    reader::SparseReader,
    writer::{SparseDelta, SparseWriter},
};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::time::Instant;
use tempfile::TempDir;
use uuid::Uuid;

// Blockfile prefix constants
const SPARSE_MAX_PREFIX: &str = "sparse_max";
const SPARSE_OFFSET_VALUE_PREFIX: &str = "sparse_offset_value";

// Sparse document and query structures using HashMap instead of sprs
#[derive(Debug, Clone)]
pub struct SparseDocument {
    pub doc_id: String,
    pub url: String,
    pub title: String,
    pub body: String,
    pub sparse_vector: HashMap<usize, f32>,
}

#[derive(Debug, Clone)]
pub struct SparseQuery {
    pub query_id: String,
    pub text: String,
    pub sparse_vector: HashMap<usize, f32>,
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

        // Create sparse vector as HashMap
        let mut sparse_vector = HashMap::new();
        for j in 0..indices.len() {
            sparse_vector.insert(indices.value(j) as usize, values.value(j));
        }

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

        // Create sparse vector as HashMap
        let mut sparse_vector = HashMap::new();
        for j in 0..indices.len() {
            sparse_vector.insert(indices.value(j) as usize, values.value(j));
        }

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
    #[arg(short = 'm', long, default_value_t = 16)]
    num_queries: usize,

    /// Top-k results to retrieve
    #[arg(short = 'k', long, default_value_t = 16)]
    top_k: usize,

    /// Block size for the sparse index
    #[arg(short = 'b', long, default_value_t = 128)]
    block_size: u32,
}

#[derive(Debug, Clone)]
struct SearchResult {
    query_id: String,
    top_k_offsets: Vec<u32>,
    scores: Vec<f32>,
    search_time_ms: f64,
}

fn compute_sparse_dot_product(vec1: &[(u32, f32)], vec2: &[(u32, f32)]) -> f32 {
    let mut score = 0.0;
    let mut i = 0;
    let mut j = 0;

    while i < vec1.len() && j < vec2.len() {
        match vec1[i].0.cmp(&vec2[j].0) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                score += vec1[i].1 * vec2[j].1;
                i += 1;
                j += 1;
            }
        }
    }

    score
}

fn brute_force_search(
    documents: &[SparseDocument],
    query: &SparseQuery,
    top_k: usize,
) -> SearchResult {
    let start = Instant::now();

    // Convert query sparse vector to sorted list of (dimension_id, value)
    let mut query_vec: Vec<(u32, f32)> = Vec::new();
    for (idx, val) in query.sparse_vector.iter() {
        query_vec.push((*idx as u32, *val));
    }
    query_vec.sort_by_key(|&(dim, _)| dim);

    // Compute scores for all documents
    let mut scores: Vec<(u32, f32)> = Vec::new();

    for (offset, doc) in documents.iter().enumerate() {
        // Convert document sparse vector to sorted list
        let mut doc_vec: Vec<(u32, f32)> = Vec::new();
        for (idx, val) in doc.sparse_vector.iter() {
            doc_vec.push((*idx as u32, *val));
        }
        doc_vec.sort_by_key(|&(dim, _)| dim);

        let score = compute_sparse_dot_product(&query_vec, &doc_vec);
        if score > 0.0 {
            scores.push((offset as u32, score));
        }
    }

    // Sort by score descending and take top-k
    scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
    scores.truncate(top_k);

    let elapsed = start.elapsed();

    SearchResult {
        query_id: query.query_id.clone(),
        top_k_offsets: scores.iter().map(|&(offset, _)| offset).collect(),
        scores: scores.iter().map(|&(_, score)| score).collect(),
        search_time_ms: elapsed.as_secs_f64() * 1000.0,
    }
}

async fn build_sparse_index(
    documents: &[SparseDocument],
    block_size: u32,
) -> anyhow::Result<(TempDir, BlockfileProvider, Uuid, Uuid)> {
    println!("Building sparse index...");
    let start = Instant::now();

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    // Create writers for the sparse index
    let max_writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(SPARSE_MAX_PREFIX.to_string()))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create max writer: {:?}", e))?;

    let offset_value_writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new(
            SPARSE_OFFSET_VALUE_PREFIX.to_string(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create offset value writer: {:?}", e))?;

    // Store the writer IDs for later use when creating readers
    let max_writer_id = max_writer.id();
    let offset_value_writer_id = offset_value_writer.id();

    // Create sparse writer (without reader since we're building from scratch)
    let sparse_writer = SparseWriter::new(block_size, max_writer, offset_value_writer, None);

    // Build the index
    let pb = ProgressBar::new(documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    // Process documents in batches for efficiency
    let batch_size = 4096;
    for (chunk_idx, chunk) in documents.chunks(batch_size).enumerate() {
        let mut delta = SparseDelta::default();

        for (idx, doc) in chunk.iter().enumerate() {
            let offset = (chunk_idx * batch_size + idx) as u32;

            // Convert sparse vector to iterator of (dimension_id, value)
            let sparse_iter = doc
                .sparse_vector
                .iter()
                .map(|(idx, val)| (*idx as u32, *val));
            delta.create(offset, sparse_iter);
        }

        // Write the batch
        sparse_writer
            .write_delta(delta)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to write delta: {:?}", e))?;
        pb.inc(chunk.len() as u64);
    }

    pb.finish_with_message("Index built");

    // Commit and flush writers
    let flusher = sparse_writer
        .commit()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to commit sparse writer: {:?}", e))?;

    flusher
        .flush()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to flush sparse writer: {:?}", e))?;

    let elapsed = start.elapsed();
    println!("Index build time: {:.2} ms", elapsed.as_secs_f64() * 1000.0);

    Ok((temp_dir, provider, max_writer_id, offset_value_writer_id))
}

async fn search_with_wand(
    provider: &BlockfileProvider,
    max_reader_id: Uuid,
    offset_value_reader_id: Uuid,
    queries: &[SparseQuery],
    top_k: usize,
) -> anyhow::Result<Vec<SearchResult>> {
    println!("\nSearching with Block-Max WAND...");

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
    let pb = ProgressBar::new(queries.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-"),
    );

    for query in queries {
        let start = Instant::now();

        // Convert query sparse vector to iterator
        let query_vec: Vec<(u32, f32)> = query
            .sparse_vector
            .iter()
            .map(|(idx, val)| (*idx as u32, *val))
            .collect();

        // Run WAND search
        let scores = sparse_reader
            .wand(query_vec, top_k as u32)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to run WAND search: {:?}", e))?;

        let elapsed = start.elapsed();

        results.push(SearchResult {
            query_id: query.query_id.clone(),
            top_k_offsets: scores.iter().map(|s| s.offset).collect(),
            scores: scores.iter().map(|s| s.score).collect(),
            search_time_ms: elapsed.as_secs_f64() * 1000.0,
        });

        pb.inc(1);
    }

    pb.finish_with_message("WAND search complete");
    Ok(results)
}

fn calculate_recall(ground_truth: &[SearchResult], predictions: &[SearchResult], k: usize) -> f32 {
    let mut total_recall = 0.0;
    let mut count = 0;

    for gt in ground_truth {
        if let Some(pred) = predictions.iter().find(|p| p.query_id == gt.query_id) {
            let gt_set: HashSet<u32> = gt.top_k_offsets.iter().take(k).cloned().collect();
            let pred_set: HashSet<u32> = pred.top_k_offsets.iter().take(k).cloned().collect();

            let intersection = gt_set.intersection(&pred_set).count();
            let recall = intersection as f32 / k.min(gt_set.len()) as f32;
            total_recall += recall;
            count += 1;
        }
    }

    if count > 0 {
        total_recall / count as f32
    } else {
        0.0
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments using clap
    let args = Args::parse();

    // Construct paths to documents and queries files
    let documents_path = format!("{}/documents.parquet", args.dataset_path);
    let queries_path = format!("{}/queries.parquet", args.dataset_path);

    println!("Sparse Index Block-Max WAND Benchmark");
    println!("======================================");
    println!("Configuration:");
    println!("  Dataset: {}", args.dataset_path);
    println!("  Documents: {}", documents_path);
    println!("  Queries: {}", queries_path);
    println!("  Num documents: {}", args.num_documents);
    println!("  Num queries: {}", args.num_queries);
    println!("  Top-k: {}", args.top_k);
    println!("  Block size: {}", args.block_size);
    println!();

    // Load documents
    println!("Loading documents...");
    let documents = load_sparse_documents(documents_path, 0, args.num_documents)?;
    println!("Loaded {} documents", documents.len());

    // Load queries
    println!("Loading queries...");
    let queries = load_sparse_queries(queries_path, 0, args.num_queries)?;
    println!("Loaded {} queries", queries.len());

    // Build sparse index
    let (temp_dir, provider, max_reader_id, offset_value_reader_id) =
        build_sparse_index(&documents, args.block_size).await?;

    // Run brute force search as ground truth
    println!("\nRunning brute force search (ground truth)...");
    let start = Instant::now();
    let brute_force_results: Vec<SearchResult> = queries
        .iter()
        .map(|query| brute_force_search(&documents, query, args.top_k))
        .collect();
    let brute_force_time = start.elapsed().as_secs_f64() * 1000.0;
    println!("Brute force total time: {:.2} ms", brute_force_time);

    // Run WAND search
    let wand_results = search_with_wand(
        &provider,
        max_reader_id,
        offset_value_reader_id,
        &queries,
        args.top_k,
    )
    .await?;

    // Calculate metrics
    let recall = calculate_recall(&brute_force_results, &wand_results, args.top_k);

    let avg_brute_force_time = brute_force_results
        .iter()
        .map(|r| r.search_time_ms)
        .sum::<f64>()
        / brute_force_results.len() as f64;

    let avg_wand_time =
        wand_results.iter().map(|r| r.search_time_ms).sum::<f64>() / wand_results.len() as f64;

    let speedup = avg_brute_force_time / avg_wand_time;

    // Print results
    println!("\n======================================");
    println!("Results:");
    println!("  Recall@{}: {:.2}%", args.top_k, recall * 100.0);
    println!("  Avg brute force time: {:.2} ms", avg_brute_force_time);
    println!("  Avg WAND time: {:.2} ms", avg_wand_time);
    println!("  Speedup: {:.2}x", speedup);

    // Print detailed comparison for first few queries
    println!("\nDetailed comparison (first 5 queries):");
    for i in 0..5.min(queries.len()) {
        let bf = &brute_force_results[i];
        let wand = &wand_results[i];

        println!("\nQuery {}: {}", i + 1, bf.query_id);
        println!(
            "  Brute force top-{} offsets: {:?}",
            args.top_k, bf.top_k_offsets
        );
        println!("  Brute force top-{} scores: {:?}", args.top_k, bf.scores);
        println!(
            "  WAND top-{} offsets: {:?}",
            args.top_k, wand.top_k_offsets
        );
        println!("  WAND top-{} scores: {:?}", args.top_k, wand.scores);

        let gt_set: HashSet<u32> = bf.top_k_offsets.iter().cloned().collect();
        let pred_set: HashSet<u32> = wand.top_k_offsets.iter().cloned().collect();
        let overlap = gt_set.intersection(&pred_set).count();

        println!("  Overlap: {}/{}", overlap, args.top_k);

        // Validate scores for matching offsets
        let mut score_differences = Vec::new();
        for (j, wand_offset) in wand.top_k_offsets.iter().enumerate() {
            if let Some(bf_idx) = bf.top_k_offsets.iter().position(|&o| o == *wand_offset) {
                let score_diff = (wand.scores[j] - bf.scores[bf_idx]).abs();
                score_differences.push(score_diff);
                if score_diff > 1e-5 {
                    println!(
                        "    Score mismatch for offset {}: BF={:.6}, WAND={:.6}, diff={:.6}",
                        wand_offset, bf.scores[bf_idx], wand.scores[j], score_diff
                    );
                }
            }
        }

        if !score_differences.is_empty() {
            let avg_diff: f32 =
                score_differences.iter().sum::<f32>() / score_differences.len() as f32;
            let max_diff = score_differences.iter().cloned().fold(0.0f32, f32::max);
            println!(
                "  Score validation: avg_diff={:.6}, max_diff={:.6}",
                avg_diff, max_diff
            );
        }

        println!("  Brute force time: {:.2} ms", bf.search_time_ms);
        println!("  WAND time: {:.2} ms", wand.search_time_ms);
    }

    // Clean up
    drop(provider);
    drop(temp_dir);

    Ok(())
}
