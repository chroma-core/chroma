//! Blockfile read-path microbenchmark for sparse posting lists.
//!
//! Measures the per-dimension cost of reading posting blocks from the
//! blockfile, isolating each layer of the read path:
//!
//! 1. `open_prefix_view` — sparse-index lookup + block loading + segment
//!    construction
//! 2. `collect_raw_binary_in_order` — linear scan over Arrow BinaryArray
//!    slices
//! 3. Block decompression — `decompress_offsets_into` + `decompress_values_into`
//! 4. Full `PostingCursor::open` — end-to-end cursor construction
//!
//! Usage:
//! ```bash
//! cargo run --release --example blockfile_read_bench -- -n 65536
//! cargo run --release --example blockfile_read_bench -- -n 500000
//! ```

use chroma_benchmark::datasets::wikipedia_splade::{SparseDocument, WikipediaSplade};
use chroma_blockstore::arrow::provider::BlockfileReaderOptions;
use chroma_blockstore::test_arrow_blockfile_provider;
use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
use chroma_index::sparse::maxscore::{
    BlockSparseReader, BlockSparseWriter, PostingCursor, SparsePostingBlock,
    SPARSE_POSTING_BLOCK_SIZE_BYTES,
};
use chroma_types::SignedRoaringBitmap;
use clap::Parser;
use futures::{StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::time::Instant;
use tempfile::TempDir;
use uuid::Uuid;

use chroma_index::sparse::types::encode_u32;

const BLOCK_MAXSCORE_PREFIX: &str = "block_maxscore";

#[derive(Parser, Debug)]
#[command(name = "blockfile_read_bench")]
struct Args {
    #[arg(short = 'n', long, default_value_t = 65536)]
    num_documents: usize,

    /// Number of dimensions to sample for the benchmark
    #[arg(short = 'd', long, default_value_t = 200)]
    num_dimensions: usize,

    /// Iterations per dimension for timing stability
    #[arg(short = 'i', long, default_value_t = 10)]
    iterations: usize,
}

async fn build_index(
    documents: &[SparseDocument],
) -> anyhow::Result<(TempDir, BlockfileProvider, Uuid)> {
    println!("Building BlockMaxMaxScore index...");
    let start = Instant::now();

    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);

    let batch_size = 65536;
    let pb = ProgressBar::new(documents.len() as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("##-"),
    );

    let mut posting_writer_id = None;

    for (chunk_idx, chunk) in documents.chunks(batch_size).enumerate() {
        let mut posting_options = BlockfileWriterOptions::new(BLOCK_MAXSCORE_PREFIX.to_string())
            .ordered_mutations()
            .max_block_size_bytes(SPARSE_POSTING_BLOCK_SIZE_BYTES);
        if let Some(id) = posting_writer_id {
            posting_options = posting_options.fork(id);
        }

        let posting_writer = provider
            .write::<u32, SparsePostingBlock>(posting_options)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create posting writer: {:?}", e))?;

        let old_reader = if let Some(id) = posting_writer_id {
            let posting_reader = provider
                .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
                    id,
                    BLOCK_MAXSCORE_PREFIX.to_string(),
                ))
                .await
                .map_err(|e| anyhow::anyhow!("Failed to create posting reader: {:?}", e))?;
            Some(BlockSparseReader::new(posting_reader))
        } else {
            None
        };

        let writer = BlockSparseWriter::new(posting_writer.clone(), old_reader);

        let base_offset = (chunk_idx * batch_size) as u32;
        for (idx, doc) in chunk.iter().enumerate() {
            let pairs: Vec<(u32, f32)> = doc
                .sparse_vector
                .indices()
                .iter()
                .zip(doc.sparse_vector.data().iter())
                .map(|(idx, val)| (*idx as u32, *val))
                .collect();
            writer.set(base_offset + idx as u32, pairs).await;
            pb.inc(1);
        }

        let flusher = writer
            .commit()
            .await
            .map_err(|e| anyhow::anyhow!("Commit failed: {:?}", e))?;
        flusher
            .flush()
            .await
            .map_err(|e| anyhow::anyhow!("Flush failed: {:?}", e))?;

        posting_writer_id = Some(posting_writer.id());

        provider
            .clear()
            .await
            .map_err(|e| anyhow::anyhow!("Clear failed: {:?}", e))?;
    }

    pb.finish_and_clear();
    println!(
        "Index built: {} docs in {:.2}s",
        documents.len(),
        start.elapsed().as_secs_f64()
    );

    Ok((
        temp_dir,
        provider,
        posting_writer_id.expect("should have writer id"),
    ))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    println!("Blockfile Read-Path Microbenchmark");
    println!("{}", "=".repeat(60));

    let dataset = WikipediaSplade::init().await?;
    let documents: Vec<_> = dataset
        .documents()
        .await?
        .take(args.num_documents)
        .try_collect()
        .await?;
    println!("Loaded {} documents", documents.len());

    // Collect dimension frequencies to pick representative dimensions
    let mut dim_freq: HashMap<u32, u32> = HashMap::new();
    for doc in &documents {
        for &idx in doc.sparse_vector.indices() {
            *dim_freq.entry(idx as u32).or_default() += 1;
        }
    }
    let mut dims_by_freq: Vec<(u32, u32)> = dim_freq.into_iter().collect();
    dims_by_freq.sort_by(|a, b| b.1.cmp(&a.1));

    println!(
        "Total unique dimensions: {}",
        dims_by_freq.len()
    );
    println!(
        "Top-10 dimensions by frequency: {:?}",
        dims_by_freq.iter().take(10).collect::<Vec<_>>()
    );

    // Sample dimensions: pick evenly across the frequency spectrum
    let sample_count = args.num_dimensions.min(dims_by_freq.len());
    let step = dims_by_freq.len() / sample_count;
    let sampled_dims: Vec<(u32, u32)> = (0..sample_count)
        .map(|i| dims_by_freq[i * step])
        .collect();

    println!(
        "Sampled {} dimensions (freq range: {} to {})",
        sampled_dims.len(),
        sampled_dims.last().map(|d| d.1).unwrap_or(0),
        sampled_dims.first().map(|d| d.1).unwrap_or(0),
    );

    let (_temp_dir, provider, posting_id) = build_index(&documents).await?;

    // Open reader
    let posting_reader = provider
        .read::<u32, SparsePostingBlock>(BlockfileReaderOptions::new(
            posting_id,
            BLOCK_MAXSCORE_PREFIX.to_string(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open reader: {:?}", e))?;

    // Prewarm all sampled prefixes
    let encoded_dims: Vec<String> = sampled_dims.iter().map(|(d, _)| encode_u32(*d)).collect();
    posting_reader
        .load_blocks_for_prefixes(encoded_dims.iter().map(|s| s.as_str()))
        .await;

    println!("\nAll blocks prewarmed. Starting measurements...\n");

    // ── Benchmark 1: open_prefix_view ──────────────────────────────
    {
        let mut total_us = 0.0;
        let mut count = 0usize;
        for encoded in &encoded_dims {
            for _ in 0..args.iterations {
                let start = Instant::now();
                let _view = posting_reader.open_prefix_view(encoded).await.unwrap();
                total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
                count += 1;
            }
        }
        let avg = total_us / count as f64;
        println!(
            "1. open_prefix_view:           {avg:>8.2} µs/call  ({count} calls)"
        );
    }

    // ── Benchmark 2: collect_raw_binary_in_order ───────────────────
    {
        let mut total_us = 0.0;
        let mut count = 0usize;
        let mut total_blocks = 0usize;
        for encoded in &encoded_dims {
            let view = posting_reader.open_prefix_view(encoded).await.unwrap();
            for _ in 0..args.iterations {
                let start = Instant::now();
                let raw = view.collect_raw_binary_in_order();
                total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
                total_blocks += raw.len();
                count += 1;
            }
        }
        let avg = total_us / count as f64;
        let avg_blocks = total_blocks as f64 / count as f64;
        println!(
            "2. collect_raw_binary:         {avg:>8.2} µs/call  (avg {avg_blocks:.1} blocks/call)"
        );
    }

    // ── Benchmark 3: block decompression ───────────────────────────
    {
        let mut total_us = 0.0;
        let mut count = 0usize;
        let mut total_entries = 0usize;
        let mut offset_buf = Vec::new();
        let mut value_buf = Vec::new();
        for encoded in &encoded_dims {
            let view = posting_reader.open_prefix_view(encoded).await.unwrap();
            let raw_blocks = view.collect_raw_binary_in_order();
            // Exclude the directory block (last one)
            let posting_blocks = if raw_blocks.len() > 1 {
                &raw_blocks[..raw_blocks.len() - 1]
            } else {
                &raw_blocks[..]
            };
            for _ in 0..args.iterations {
                for raw in posting_blocks {
                    let start = Instant::now();
                    let hdr = SparsePostingBlock::peek_header(raw);
                    SparsePostingBlock::decompress_offsets_into(raw, &hdr, &mut offset_buf);
                    SparsePostingBlock::decompress_values_into(raw, &hdr, &mut value_buf);
                    total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
                    total_entries += offset_buf.len();
                    count += 1;
                }
            }
        }
        let avg = total_us / count as f64;
        let avg_entries = total_entries as f64 / count as f64;
        println!(
            "3. decompress block:           {avg:>8.2} µs/block (avg {avg_entries:.0} entries/block)"
        );
    }

    // ── Benchmark 4: full PostingCursor::open ──────────────────────
    {
        let mut total_us = 0.0;
        let mut count = 0usize;
        for encoded in &encoded_dims {
            for _ in 0..args.iterations {
                let start = Instant::now();
                let _cursor = PostingCursor::open(&posting_reader, encoded.clone())
                    .await
                    .unwrap();
                total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
                count += 1;
            }
        }
        let avg = total_us / count as f64;
        println!(
            "4. PostingCursor::open:        {avg:>8.2} µs/call  ({count} calls)"
        );
    }

    // ── Benchmark 5: cursor sequential scan (next loop) ────────────
    {
        let mask = SignedRoaringBitmap::full();
        let mut total_us = 0.0;
        let mut total_entries = 0usize;
        let mut count = 0usize;
        for encoded in &encoded_dims {
            let mut cursor = PostingCursor::open(&posting_reader, encoded.clone())
                .await
                .unwrap()
                .unwrap();
            cursor.advance(0, &mask);
            let start = Instant::now();
            let mut n = 0usize;
            while cursor.current().is_some() {
                cursor.next();
                n += 1;
            }
            total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
            total_entries += n;
            count += 1;
        }
        let avg = total_us / count as f64;
        let avg_entries = total_entries as f64 / count as f64;
        let ns_per_entry = if total_entries > 0 {
            (total_us * 1000.0) / total_entries as f64
        } else {
            0.0
        };
        println!(
            "5. sequential scan (next):     {avg:>8.2} µs/dim   (avg {avg_entries:.0} entries, {ns_per_entry:.1} ns/entry)"
        );
    }

    // ── Benchmark 6: cursor advance with skip ──────────────────────
    {
        let mask = SignedRoaringBitmap::full();
        let mut total_us = 0.0;
        let mut total_advances = 0usize;
        let mut count = 0usize;
        for (dim_id, _freq) in &sampled_dims {
            let encoded = encode_u32(*dim_id);
            let mut cursor = PostingCursor::open(&posting_reader, encoded)
                .await
                .unwrap()
                .unwrap();
            // advance to every 100th doc
            let stride = 100u32;
            let start = Instant::now();
            let mut target = 0u32;
            loop {
                let result = cursor.advance(target, &mask);
                if result.is_none() {
                    break;
                }
                total_advances += 1;
                target += stride;
            }
            total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
            count += 1;
        }
        let avg = total_us / count as f64;
        let avg_advances = total_advances as f64 / count as f64;
        let ns_per_advance = if total_advances > 0 {
            (total_us * 1000.0) / total_advances as f64
        } else {
            0.0
        };
        println!(
            "6. advance (stride=100):       {avg:>8.2} µs/dim   (avg {avg_advances:.0} advances, {ns_per_advance:.1} ns/advance)"
        );
    }

    // ── Benchmark 7: get_value point lookups (random access) ───────
    {
        let mask = SignedRoaringBitmap::full();
        let mut total_us = 0.0;
        let mut total_lookups = 0usize;
        let mut count = 0usize;
        for (dim_id, _) in &sampled_dims {
            let encoded = encode_u32(*dim_id);
            // First pass: collect all doc IDs for this dimension
            let mut cursor = PostingCursor::open(&posting_reader, encoded.clone())
                .await
                .unwrap()
                .unwrap();
            cursor.advance(0, &mask);
            let mut doc_ids = Vec::new();
            while let Some((doc, _)) = cursor.current() {
                doc_ids.push(doc);
                cursor.next();
            }

            if doc_ids.is_empty() {
                continue;
            }

            // Point-lookup every 10th doc
            let mut cursor = PostingCursor::open(&posting_reader, encode_u32(*dim_id))
                .await
                .unwrap()
                .unwrap();
            let stride = 10.max(doc_ids.len() / 100);
            let targets: Vec<u32> = doc_ids.iter().step_by(stride).copied().collect();

            let start = Instant::now();
            for &doc in &targets {
                let _ = cursor.get_value(doc);
            }
            total_us += start.elapsed().as_secs_f64() * 1_000_000.0;
            total_lookups += targets.len();
            count += 1;
        }
        let avg = total_us / count as f64;
        let avg_lookups = total_lookups as f64 / count as f64;
        let ns_per_lookup = if total_lookups > 0 {
            (total_us * 1000.0) / total_lookups as f64
        } else {
            0.0
        };
        println!(
            "7. get_value (point lookup):   {avg:>8.2} µs/dim   (avg {avg_lookups:.0} lookups, {ns_per_lookup:.1} ns/lookup)"
        );
    }

    println!("\nDone.");
    Ok(())
}
