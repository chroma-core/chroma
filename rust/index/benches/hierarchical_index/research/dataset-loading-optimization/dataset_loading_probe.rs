#![allow(dead_code)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
mod datasets;
use datasets::Dataset;
use std::time::Instant;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let name = args.get(1).map(String::as_str).unwrap_or("msmarco");
    let mode = args.get(2).map(String::as_str).unwrap_or("compare");
    let count: usize = args.get(3).map(|v| v.parse().unwrap()).unwrap_or(1_000_000);
    let baseline: Box<dyn Dataset> = match name {
        "msmarco" => Box::new(datasets::baseline_msmarco::MsMarco::load().await?),
        _ => Box::new(datasets::baseline_wikipedia::Wikipedia::load().await?),
    };
    let candidate: Box<dyn Dataset> = match name {
        "msmarco" => Box::new(datasets::msmarco::MsMarco::load().await?),
        _ => Box::new(datasets::wikipedia::Wikipedia::load().await?),
    };
    let mut total = 0.;
    let mut digest = 0xcbf29ce484222325u64;
    for offset in (0..count).step_by(100_000) {
        let loader = if mode == "baseline" { &baseline } else { &candidate };
        let start = Instant::now();
        let rows = loader.load_range(offset, 100_000.min(count - offset))?;
        let elapsed = start.elapsed().as_secs_f64();
        total += elapsed;
        println!("chunk dataset={name} mode={mode} offset={offset} rows={} seconds={elapsed:.6}", rows.len());
        if mode == "compare" {
            let expected = baseline.load_range(offset, 100_000.min(count - offset))?;
            assert_eq!(expected.len(), rows.len());
            for ((id, v), (expected_id, expected_v)) in rows.iter().zip(expected.iter()) {
                assert_eq!(id, expected_id);
                assert_eq!(v.len(), expected_v.len());
                for (x, y) in v.iter().zip(expected_v.iter()) { assert_eq!(x.to_bits(), y.to_bits()); }
            }
        }
        for (id, vector) in rows {
            digest = (digest ^ id as u64).wrapping_mul(0x100000001b3);
            for value in vector.iter() { digest = (digest ^ value.to_bits() as u64).wrapping_mul(0x100000001b3); }
        }
    }
    println!("total dataset={name} mode={mode} vectors={count} seconds={total:.6} digest={digest:016x}");
    Ok(())
}
