use chroma_blockstore::{test_arrow_blockfile_provider, BlockfileWriterOptions};
use chroma_index::sparse::{reader::SparseReader, writer::SparseWriter};
use chroma_types::SignedRoaringBitmap;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::time::Instant;

fn generate_synthetic_data(
    rng: &mut StdRng,
    num_docs: u32,
    num_dimensions: u32,
    avg_nnz_per_doc: usize,
) -> Vec<(u32, Vec<(u32, f32)>)> {
    (0..num_docs)
        .map(|offset| {
            let nnz = rng.gen_range(1..=avg_nnz_per_doc * 2);
            let mut dims: Vec<u32> = (0..nnz)
                .map(|_| rng.gen_range(0..num_dimensions))
                .collect();
            dims.sort_unstable();
            dims.dedup();
            let pairs: Vec<(u32, f32)> = dims
                .into_iter()
                .map(|d| (d, rng.gen_range(0.01..1.0)))
                .collect();
            (offset, pairs)
        })
        .collect()
}

fn generate_query(rng: &mut StdRng, num_dimensions: u32, nnz: usize) -> Vec<(u32, f32)> {
    let mut dims: Vec<u32> = (0..nnz)
        .map(|_| rng.gen_range(0..num_dimensions))
        .collect();
    dims.sort_unstable();
    dims.dedup();
    dims.into_iter()
        .map(|d| (d, rng.gen_range(0.01..1.0)))
        .collect()
}

async fn build_reader(
    vectors: Vec<(u32, Vec<(u32, f32)>)>,
) -> (tempfile::TempDir, SparseReader<'static>) {
    let (temp_dir, provider) = test_arrow_blockfile_provider(8 * 1024 * 1024);
    let max_writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()).ordered_mutations())
        .await
        .unwrap();
    let offset_value_writer = provider
        .write::<u32, f32>(BlockfileWriterOptions::new("".to_string()).ordered_mutations())
        .await
        .unwrap();
    let max_id = max_writer.id();
    let offset_value_id = offset_value_writer.id();

    let writer = SparseWriter::new(64, max_writer, offset_value_writer, None);
    for (offset, vector) in vectors {
        writer.set(offset, vector).await;
    }
    let flusher = Box::pin(writer.commit()).await.unwrap();
    Box::pin(flusher.flush()).await.unwrap();

    let max_reader = provider
        .read::<u32, f32>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
            max_id,
            "".to_string(),
        ))
        .await
        .unwrap();
    let offset_value_reader = provider
        .read::<u32, f32>(chroma_blockstore::arrow::provider::BlockfileReaderOptions::new(
            offset_value_id,
            "".to_string(),
        ))
        .await
        .unwrap();

    let reader = SparseReader::new(max_reader, offset_value_reader);
    (temp_dir, reader)
}

async fn run_bench(
    reader: &SparseReader<'_>,
    queries: &[Vec<(u32, f32)>],
    k: u32,
    mask: SignedRoaringBitmap,
    label: &str,
) {
    let num_queries = queries.len();

    // Warmup
    for q in queries.iter().take(5) {
        let _ = reader.wand(q.clone(), k, mask.clone()).await.unwrap();
    }

    // 3 timed runs, take the best
    let mut best = f64::MAX;
    for _ in 0..3 {
        let start = Instant::now();
        for q in queries {
            let _ = reader.wand(q.clone(), k, mask.clone()).await.unwrap();
        }
        let elapsed_us = start.elapsed().as_micros() as f64 / num_queries as f64;
        best = best.min(elapsed_us);
    }
    eprintln!("  {label:<20} {best:>8.0}µs/query  (best of 3 runs, {num_queries} queries)");
}

#[tokio::main]
async fn main() {
    let mut rng = StdRng::seed_from_u64(42);
    let num_docs = 10_000u32;
    let num_dimensions = 30_000u32;
    let avg_nnz = 50;
    let num_queries = 50;
    let k = 10u32;

    eprintln!("Building index: {num_docs} docs, {num_dimensions} dims, ~{avg_nnz} nnz/doc...");
    let vectors = generate_synthetic_data(&mut rng, num_docs, num_dimensions, avg_nnz);
    let queries: Vec<Vec<(u32, f32)>> = (0..num_queries)
        .map(|_| generate_query(&mut rng, num_dimensions, 30))
        .collect();

    let (_temp_dir, reader) = build_reader(vectors).await;

    eprintln!("\nResults (release mode, best of 3):");
    eprintln!("{}", "-".repeat(55));

    run_bench(&reader, &queries, k, SignedRoaringBitmap::full(), "full_mask").await;

    let mut rbm = roaring::RoaringBitmap::new();
    for i in 0..num_docs {
        if i % 2 == 0 {
            rbm.insert(i);
        }
    }
    let mask = SignedRoaringBitmap::Include(rbm);
    run_bench(&reader, &queries, k, mask, "include_50pct").await;
}
