#![recursion_limit = "256"]
use std::sync::Arc;
use std::time::{Duration, Instant};

use guacamole::combinators::*;
use guacamole::Guacamole;

use chroma_config::{registry::Registry, Configurable};
use chroma_storage::config::{S3CredentialsConfig, S3StorageConfig, StorageConfig};

use wal3::{Error, LogWriter, LogWriterOptions};

///////////////////////////////////////////// benchmark ////////////////////////////////////////////

#[derive(Clone, Eq, PartialEq)]
pub struct Options {
    pub path: String,
    pub runtime: usize,
    pub target_throughput: usize,
    pub max_tokio_tasks: usize,
    pub log: LogWriterOptions,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            path: "wal3.data".to_string(),
            runtime: 60,
            target_throughput: 1_000,
            max_tokio_tasks: 10_000,
            log: LogWriterOptions::default(),
        }
    }
}

async fn append_once(mut guac: Guacamole, log: Arc<LogWriter>) {
    let mut record = vec![0; 1 << 13];
    guac.generate(&mut record);
    log.append(record).await.unwrap();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let options = Options::default();

    // Setup the storage.
    let storage_config = StorageConfig::S3(S3StorageConfig {
        bucket: "chroma-storage".to_string(),
        credentials: S3CredentialsConfig::Minio,
        ..Default::default()
    });

    let registry = Registry::default();
    let storage = Arc::new(
        Configurable::try_from_config(&storage_config, &registry)
            .await
            .unwrap(),
    );

    // NOTE(rescrv):  Outside benchmarking we don't want to initialize except when we create a new
    // log.  A durability event that loses the manifest will cause the log to become truncated.
    // Recovery is necessary, not just creating the manifest.
    match LogWriter::initialize(&options.log, &storage, "wal3bench", "benchmark initializer").await
    {
        Ok(_) => {}
        Err(Error::AlreadyInitialized) => {}
        Err(e) => {
            eprintln!("error initializing log: {e:?}");
            std::process::exit(1);
        }
    };
    let log = Arc::new(
        LogWriter::open(
            options.log.clone(),
            storage,
            "wal3bench",
            "benchmark writer",
            (),
        )
        .await
        .unwrap(),
    );

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
            panic!("benchmark task queue full");
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
    Arc::into_inner(log).unwrap().close().await.unwrap();
    println!("log closed");
}
