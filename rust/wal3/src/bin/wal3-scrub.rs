use std::sync::Arc;

use chroma_config::{registry::Registry, Configurable};
use chroma_storage::config::{S3CredentialsConfig, S3StorageConfig, StorageConfig};

use wal3::{Error, LogReader, LogReaderOptions};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
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

    let mut good = true;

    // For each prefix in the configured s3 bucket, open a log reader and scrub.
    for log in std::env::args().skip(1) {
        let prefix = log.clone();
        let log = LogReader::open(LogReaderOptions::default(), Arc::clone(&storage), log)
            .await
            .unwrap();
        match log.scrub().await {
            Ok(_) => eprintln!("Scrubbed log {}", prefix),
            Err(Error::UninitializedLog) => {
                good = false;
                eprintln!("Uninitialized log {}", prefix);
            }
            Err(Error::ScrubError(scrub)) => {
                good = false;
                // NOTE(rescrv):  This is the only type we expect to process on out of scrub, so
                // output it to stdout instead of stderr.  A SUCCESS exit status plus no stderr
                // means success.
                println!("{}", scrub);
            }
            Err(e) => {
                good = false;
                eprintln!("Error scrubbing log {}: {}", prefix, e);
            }
        }
    }

    // If we had an error, exit abnormally.
    if !good {
        std::process::exit(13);
    }
}
