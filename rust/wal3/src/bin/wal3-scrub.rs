use arrrg::CommandLine;
use biometrics::Sensor;
//use object_store::aws::{AmazonS3, AmazonS3Builder, S3ConditionalPut};

use wal3::{LogReader, LogReaderOptions};

#[derive(Clone, Eq, PartialEq, arrrg_derive::CommandLine)]
pub struct Options {
    #[arrrg(optional, "Path to the object store.")]
    pub path: String,
    #[arrrg(nested)]
    pub log: LogReaderOptions,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            path: "wal3.data".to_string(),
            log: LogReaderOptions::default(),
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let (options, free) = Options::from_command_line_relaxed("USAGE: wal3 [OPTIONS]");
    if !free.is_empty() {
        eprintln!("command takes no positional arguments");
        std::process::exit(1);
    }

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
    let mut log = LogReader::open(options.log.clone(), object_store)
        .await
        .unwrap();
    log.scrub().await.unwrap();
    let reading = wal3::LOG_TTFB_LATENCY.read();
    println!(
        "LOG_TTFB_LATENCY: {} {} {}",
        reading.n(),
        reading.mean(),
        reading.variance()
    );
    let reading = wal3::LOG_FETCH_LATENCY.read();
    println!(
        "LOG_FETCH_LATENCY: {} {} {}",
        reading.n(),
        reading.mean(),
        reading.variance()
    );
}
