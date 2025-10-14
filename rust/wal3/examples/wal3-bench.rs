#![recursion_limit = "256"]
use std::sync::Arc;
use std::time::{Duration, Instant};

use guacamole::combinators::*;
use guacamole::Guacamole;

use chroma_storage::s3::s3_client_for_test_with_bucket_name;
use chroma_storage::Storage;

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
    match log.append(record).await {
        Ok(_)
        | Err(Error::LogContentionDurable)
        | Err(Error::LogContentionFailure)
        | Err(Error::LogContentionRetry) => {}
        Err(_err) => {
            //println!("err {}:{}: {err:?}", file!(), line!());
        }
    }
}

async fn garbage_collect_in_a_loop(options: LogWriterOptions, storage: Arc<Storage>, prefix: &str) {
    loop {
        let log = match LogWriter::open(
            options.clone(),
            Arc::clone(&storage),
            prefix,
            "benchmark gc'er",
            (),
        )
        .await
        {
            Ok(log) => log,
            Err(_) => {
                // squash this error for better log debuggability
                continue;
            }
        };
        if let Err(_err) = garbage_collect_once(&log).await {
            //println!("err {}:{}: {err:?}", file!(), line!());
        }
    }
}

async fn garbage_collect_once(_log: &LogWriter) -> Result<(), Error> {
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let options = Options::default();

    let storage = Arc::new(s3_client_for_test_with_bucket_name("wal3-testing").await);

    let log = Arc::new(
        LogWriter::open_or_initialize(
            options.log.clone(),
            Arc::clone(&storage),
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
    let gcer = tokio::task::spawn(garbage_collect_in_a_loop(
        options.log.clone(),
        Arc::clone(&storage),
        "wal3bench",
    ));
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
            println!("max tokio tasks exceeded: {tasks_alive}");
            break;
        }
    }
    println!("done offering load");
    println!("{:?}", log.count_waiters());
    let drained = Instant::now();
    drop(tx);
    println!("{}", log.debug_dump());
    reaper.await.unwrap();
    println!("done with benchmark");
    gcer.abort();
    let closed = Instant::now();
    println!("closing");
    Arc::into_inner(log).unwrap().close().await.unwrap();
    println!(
        "log drained in {:?} closed in {:?}",
        drained.elapsed(),
        closed.elapsed()
    );
}
