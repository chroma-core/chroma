use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use arrrg::CommandLine;
use biometrics::{Collector, Counter, Histogram, Moments};
use guacamole::combinators::*;
use guacamole::Guacamole;
//use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};
use object_store::ObjectStore;
use utf8path::Path;
use uuid::uuid;

use wal3::{
    Error, LatencyControlledObjectStore, LogWriter, LogWriterOptions, Message, RobustObjectStore,
    SimulationOptions, StreamID,
};

///////////////////////////////////////////// constants ////////////////////////////////////////////

const STREAM: StreamID = StreamID(uuid!("6842eead-7f5a-4eb5-9583-2e626f7424f1"));

//////////////////////////////////////////// biometrics ////////////////////////////////////////////

static RECORDS_GENERATED: Counter = Counter::new("wal3.benchmark.records_generated");

static TARGET_LATENCY: Moments = Moments::new("wal3.benchmark.target_latency");
static APPEND_LATENCY: Moments = Moments::new("wal3.benchmark.append_latency");

static APPEND_HISTOGTRAM_IMPL: sig_fig_histogram::LockFreeHistogram<487> =
    sig_fig_histogram::LockFreeHistogram::new(2);
static APPEND_HISTOGRAM: Histogram =
    Histogram::new("wal3.benchmark.append_histogram", &APPEND_HISTOGTRAM_IMPL);

static BENCHMARK_TOO_SLOW: Counter = Counter::new("wal3.benchmark.too_slow");

fn register_biometrics(collector: &Collector) {
    collector.register_counter(&RECORDS_GENERATED);

    collector.register_moments(&TARGET_LATENCY);
    collector.register_moments(&APPEND_LATENCY);

    collector.register_histogram(&APPEND_HISTOGRAM);

    collector.register_counter(&BENCHMARK_TOO_SLOW);
}

///////////////////////////////////////////// benchmark ////////////////////////////////////////////

#[derive(Clone, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct Options {
    #[arrrg(optional, "Path to the object store.")]
    pub path: String,
    #[arrrg(optional, "Number of seconds for which to run the benchmark.")]
    pub runtime: usize,
    #[arrrg(
        optional,
        "Target throughput in records per second across all threads."
    )]
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
            target_throughput: 1_000,
            max_tokio_tasks: 10_000,
            object_store: SimulationOptions::default(),
            log: LogWriterOptions::default(),
        }
    }
}

async fn append_once(mut guac: Guacamole, log: Arc<LogWriter<impl ObjectStore>>) {
    let mut record = vec![0; 1 << 13];
    guac.generate(&mut record);
    RECORDS_GENERATED.click();
    let start = Instant::now();
    log.append(STREAM, Message::Payload(record)).await.unwrap();
    let elapsed = start.elapsed();
    APPEND_LATENCY.add(elapsed.as_millis() as f64);
    APPEND_HISTOGRAM.observe(elapsed.as_millis() as f64);
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
    let emitter = Arc::new(Mutex::new(biometrics_prometheus::Emitter::new(
        bio_prom_options,
    )));
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        let mut emitter = emitter.lock().unwrap();
        collector
            .emit(
                &mut *emitter,
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
            )
            .unwrap();
    });

    // setup the log
    /*
    let object_store: AmazonS3 = AmazonS3Builder::from_env()
        .with_bucket_name("chroma-robert-wal3-test-bucket")
        .with_region("us-east-2")
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .unwrap();
    */
    let object_store = object_store::local::LocalFileSystem::new_with_prefix(options.path).unwrap();
    let object_store = LatencyControlledObjectStore::new(
        options.object_store.clone(),
        object_store,
        Guacamole::new(0),
    );
    let object_store = RobustObjectStore::new(object_store);
    // NOTE(rescrv):  Outside benchmarking we don't want to initialize except when we create a new
    // log.  A durability event that loses the manifest will cause the log to become truncated.
    // Recovery is necessary, not just creating the manifest.
    match LogWriter::initialize(&options.log, &object_store).await {
        Ok(_) => {}
        Err(Error::AlreadyInitialized) => {}
        Err(e) => {
            eprintln!("error initializing log: {e:?}");
            std::process::exit(1);
        }
    };
    let log = LogWriter::open(options.log.clone(), object_store)
        .await
        .unwrap();
    log.open_stream(STREAM).await.unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(options.target_throughput + 1_000_000);
    tx.send(tokio::task::spawn(async move {})).await.unwrap();
    let reaper = tokio::task::spawn(async move {
        while let Some(handle) = rx.recv().await {
            handle.await.unwrap();
        }
    });
    let mut guac = Guacamole::new(0);
    let start = Instant::now();
    let mut next = Duration::ZERO;
    loop {
        let gap = interarrival_duration(options.target_throughput as f64)(&mut guac);
        TARGET_LATENCY.add(gap.as_micros() as f64);
        next += gap;
        let elapsed = start.elapsed();
        if elapsed > Duration::from_secs(options.runtime as u64) {
            break;
        } else if elapsed < next {
            tokio::time::sleep(next - elapsed).await;
        }
        let log = Arc::clone(&log);
        let seed = any::<u64>(&mut guac);
        if tx
            .try_send(tokio::task::spawn(async move {
                append_once(Guacamole::new(seed), log).await
            }))
            .is_err()
        {
            BENCHMARK_TOO_SLOW.click();
        }
        let tasks_alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        if tasks_alive > options.max_tokio_tasks {
            eprintln!("max tokio tasks exceeded: {tasks_alive}");
            break;
        }
    }
    println!("done offering load");
    drop(tx);
    reaper.await.unwrap();
    println!("done with benchmark");
    log.close().await.unwrap();
    println!("log closed");
}
