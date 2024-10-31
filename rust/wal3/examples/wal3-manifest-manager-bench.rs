use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arrrg::CommandLine;
use biometrics::{Collector, Histogram};
use guacamole::combinators::*;
use guacamole::Guacamole;
//use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use utf8path::Path;

use wal3::{
    LatencyControlledObjectStore, LogWriterOptions, Manifest, ManifestManager, ShardFragment,
    ShardID, ShardSeqNo, SimulationOptions,
};

//////////////////////////////////////////// biometrics ////////////////////////////////////////////

static APPLY_LATENCY_IMPL: sig_fig_histogram::LockFreeHistogram<1000> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static APPLY_LATENCY: Histogram =
    Histogram::new("wal3__benchmark__apply_histogram", &APPLY_LATENCY_IMPL);

fn register_biometrics(collector: &Collector) {
    collector.register_histogram(&APPLY_LATENCY);
}

///////////////////////////////////////////// benchmark ////////////////////////////////////////////

#[derive(Clone, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct Options {
    #[arrrg(optional, "Path to the object store.")]
    pub path: String,
    #[arrrg(optional, "Number of seconds for which to run the benchmark.")]
    pub runtime: usize,
    #[arrrg(optional, "Target throughput in fragments per second per shard.")]
    pub target_throughput: usize,
    #[arrrg(optional, "Maximum number of tokio tasks to spawn.")]
    pub max_tokio_tasks: usize,
    #[arrrg(nested)]
    pub object_store: SimulationOptions,
    #[arrrg(nested)]
    pub log: LogWriterOptions,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            path: "wal3.data".to_string(),
            runtime: 60,
            target_throughput: 3_500,
            max_tokio_tasks: 100_000,
            object_store: SimulationOptions::default(),
            log: LogWriterOptions::default(),
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let (options, free) = Options::from_command_line_relaxed("USAGE: wal3 [OPTIONS]");
    if !free.is_empty() {
        eprintln!("command takes no positional arguments");
        std::process::exit(1);
    }
    // setup biometrics
    let collector = Collector::new();
    register_biometrics(&collector);
    wal3::register_biometrics(&collector);
    let bio_prom_options = biometrics_prometheus::Options {
        segment_size: 1 << 24,
        flush_interval: Duration::from_secs(30),
        prefix: Path::from("wal3.").into_owned(),
    };
    let mut emitter = biometrics_prometheus::Emitter::new(bio_prom_options);
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        collector
            .emit(
                &mut emitter,
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )
            .unwrap();
    });

    // setup the manifest manager
    /*
    let object_store: AmazonS3 = AmazonS3Builder::from_env()
        .with_bucket_name("chroma-robert-wal3-test-bucket")
        .with_region("us-east-2")
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .unwrap();
    */
    let object_store =
        object_store::local::LocalFileSystem::new_with_prefix(&options.path).unwrap();
    let object_store = LatencyControlledObjectStore::new(
        options.object_store.clone(),
        object_store,
        Guacamole::new(0),
    );
    let object_store = Arc::new(object_store);
    Manifest::initialize(&options.log, &*object_store)
        .await
        .expect("manifest to initialize");
    let manifest = Manifest::load(&*object_store, options.log.load_alpha)
        .await
        .expect("manifest to load")
        .expect("manifest to exist");
    let manifest_manager = Arc::new(
        ManifestManager::new(
            options.log.throttle_manifest,
            options.log.snapshot_manifest,
            manifest,
            Arc::clone(&object_store),
        )
        .await,
    );

    // Spawn the threads.
    let mut threads = vec![];
    for idx in 1..=options.log.shards {
        let options = options.clone();
        let manifest_manager = Arc::clone(&manifest_manager);
        threads.push(tokio::task::spawn(async move {
            let mut guac = Guacamole::new(unique_set_index(0xc0ffee)(idx) as u64);
            let start = Instant::now();
            let mut next = Duration::ZERO;
            let shard_id = ShardID(idx);
            let mut shard_seq_no = ShardSeqNo(1);
            loop {
                let gap = interarrival_duration(options.target_throughput as f64)(&mut guac);
                next += gap;
                let elapsed = start.elapsed();
                if elapsed > Duration::from_secs(options.runtime as u64) {
                    break;
                } else if elapsed < next {
                    tokio::time::sleep(next - elapsed).await;
                }
                let start = Instant::now();
                let (log_position, delta_seq_no) = manifest_manager
                    .assign_timestamp(1)
                    .expect("log should never fill");
                let delta = ShardFragment {
                    path: "doesn't matter".to_string(),
                    shard_id,
                    seq_no: shard_seq_no,
                    start: log_position,
                    limit: log_position + 1,
                    setsum: sst::Setsum::default(),
                };
                manifest_manager
                    .apply_delta(delta, delta_seq_no)
                    .await
                    .expect("apply delta to succeed");
                let elapsed = start.elapsed();
                APPLY_LATENCY.observe(elapsed.as_micros() as f64);
                let handle = tokio::runtime::Handle::current();
                let tasks_alive = handle.metrics().num_alive_tasks();
                if tasks_alive > options.max_tokio_tasks {
                    eprintln!("max tokio tasks exceeded: {tasks_alive}");
                    break;
                }
                shard_seq_no += 1;
            }
        }));
    }
    // Wait for the threads to finish.
    for thread in threads {
        let _ = thread.await;
    }
}
