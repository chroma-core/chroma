#![recursion_limit = "256"]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DurationRound, Utc};
use guacamole::combinators::*;
use guacamole::Guacamole;

use chroma_storage::s3::s3_client_for_test_with_bucket_name;
use uuid::Uuid;

use s3heap::{
    Error, HeapScheduler, HeapWriter, Schedule, Triggerable, UnitOfPartitioningUuid,
    UnitOfSchedulingUuid,
};

///////////////////////////////////////////// DummyScheduler ///////////////////////////////////////

struct DummyScheduler;

#[async_trait::async_trait]
impl HeapScheduler for DummyScheduler {
    async fn are_done(&self, items: &[(Triggerable, uuid::Uuid)]) -> Result<Vec<bool>, Error> {
        Ok(vec![false; items.len()])
    }

    async fn get_schedules(&self, _ids: &[uuid::Uuid]) -> Result<Vec<Schedule>, Error> {
        Ok(vec![])
    }
}

///////////////////////////////////////////// benchmark ////////////////////////////////////////////

#[derive(Clone, Eq, PartialEq)]
pub struct Options {
    pub runtime: usize,
    pub target_throughput: usize,
    pub max_tokio_tasks: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            runtime: 60,
            target_throughput: 100_000,
            max_tokio_tasks: 10_000_000,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let options = Options::default();
    let storage = s3_client_for_test_with_bucket_name("s3heap-testing").await;
    let heap = Arc::new(
        HeapWriter::new(storage, "s3heapbench".to_string(), Arc::new(DummyScheduler))
            .await
            .unwrap(),
    );
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<Schedule>(options.target_throughput + options.max_tokio_tasks);
    let count = Arc::new(AtomicU64::new(0));
    let sum = Arc::new(AtomicU64::new(0));
    let heap_count = Arc::clone(&count);
    let heap_sum = Arc::clone(&sum);
    let heap_runner: Arc<HeapWriter> = Arc::clone(&heap);
    let runner = tokio::task::spawn(async move {
        let mut buffer = vec![];
        loop {
            if rx
                .recv_many(
                    &mut buffer,
                    options.target_throughput + options.max_tokio_tasks,
                )
                .await
                == 0
            {
                break;
            }
            eprintln!("HEAP::PUSH {}", buffer.len());
            heap_runner.push(&buffer).await.unwrap();
            heap_count.fetch_add(1, Ordering::Relaxed);
            heap_sum.fetch_add(buffer.len().try_into().unwrap(), Ordering::Relaxed);
            buffer.clear()
        }
    });
    let mut guac = Guacamole::new(0);
    let start = Instant::now();
    let mut next = Duration::ZERO;
    loop {
        let gap = interarrival_duration(options.target_throughput as f64)(&mut guac);
        // This is so that we'll put it approximately a minute in the future on average, but with
        // an expontential long tail.
        let future = interarrival_duration(1.0 / 60.0)(&mut guac);
        next += gap;
        let elapsed = start.elapsed();
        if elapsed > Duration::from_secs(options.runtime as u64) {
            break;
        } else if elapsed < next {
            tokio::time::sleep(next - elapsed).await;
        }
        let uuid = Uuid::new_v4();
        let nonce = Uuid::new_v4();
        if tx
            .try_send(Schedule {
                triggerable: Triggerable {
                    partitioning: UnitOfPartitioningUuid::new(Uuid::new_v4()),
                    scheduling: UnitOfSchedulingUuid::new(uuid),
                },
                nonce,
                next_scheduled: Utc::now()
                    .duration_round(chrono::TimeDelta::from_std(future).unwrap())
                    .unwrap(),
            })
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
    println!(
        "done offering load {:?} {} operations in {} batches",
        start.elapsed(),
        sum.load(Ordering::Relaxed),
        count.load(Ordering::Relaxed),
    );
    let drained = Instant::now();
    drop(tx);
    runner.await.unwrap();
    println!(
        "done with benchmark {:?}/{:?} {} operations in {} batches",
        drained.elapsed(),
        start.elapsed(),
        sum.load(Ordering::Relaxed),
        count.load(Ordering::Relaxed),
    );
}
