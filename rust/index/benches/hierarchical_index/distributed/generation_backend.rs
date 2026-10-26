//! Conditional immutable objects use the existing storage client. This adapter
//! reads current write-once keys with integrity checks; it does not pin VersionId.
use super::generation::{GenerationError, Result};
use chroma_config::{registry::Registry, Configurable};
use chroma_storage::{
    config::{S3CredentialsConfig, S3StorageConfig, StorageConfig},
    ETag, GetOptions, PutMode, PutOptions, Storage, StorageError,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableStoreConfig {
    pub bucket: String,
    /// A fresh task/index namespace beneath the retained authorized prefix.
    pub prefix: String,
}
impl DurableStoreConfig {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty()
            || !self
                .bucket
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'.')
            || !self.prefix.starts_with("sha256/")
            || self.prefix.len() <= 7
            || self.prefix.split('/').any(|s| {
                s.is_empty()
                    || s == "."
                    || s == ".."
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            })
        {
            return Err(GenerationError::Invalid(
                "durable store requires a bucket and task-unique sha256/ namespace".into(),
            ));
        }
        Ok(())
    }
    pub(super) fn connect(&self) -> Result<Arc<dyn ConditionalObjects>> {
        self.validate()?;
        #[cfg(test)]
        if let Some(backend) = mock::lookup(self) {
            return Ok(backend);
        }
        static RUNTIME: OnceLock<Arc<tokio::runtime::Runtime>> = OnceLock::new();
        let runtime = RUNTIME
            .get_or_init(|| {
                // Writers keep two workers. A reader issuing many parallel
                // posting fetches can raise this; the value is read once.
                let workers = std::env::var("SPANN_STORAGE_RUNTIME_WORKERS")
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .filter(|n| (1..=64).contains(n))
                    .unwrap_or(2);
                Arc::new(
                    tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(workers)
                        .enable_all()
                        .build()
                        .expect("storage runtime"),
                )
            })
            .clone();
        let config = StorageConfig::S3(S3StorageConfig {
            bucket: self.bucket.clone(),
            credentials: S3CredentialsConfig::AWS,
            ..Default::default()
        });
        let storage = runtime
            .block_on(Storage::try_from_config(&config, &Registry::default()))
            .map_err(|e| GenerationError::Invalid(format!("storage configuration: {e}")))?;
        Ok(Arc::new(StorageObjects {
            storage,
            runtime,
            prefix: self.prefix.clone(),
        }))
    }
}
pub(super) struct ObjectBytes {
    pub bytes: Vec<u8>,
    pub etag: String,
}
/// Only the selected S3 adapter and the conditional test model implement this
/// seam. Storage::Local ignores conditional options and is never used here.
pub(super) trait ConditionalObjects: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<ObjectBytes>>;
    /// None means create-if-absent; Some means replace exactly that ETag.
    fn put(&self, key: &str, bytes: &[u8], expected: Option<&str>) -> Result<String>;
}
struct StorageObjects {
    storage: Storage,
    runtime: Arc<tokio::runtime::Runtime>,
    prefix: String,
}
fn storage_error(e: StorageError) -> GenerationError {
    match e {
        StorageError::Precondition { .. } | StorageError::AlreadyExists { .. } => {
            GenerationError::Conflict
        }
        other => GenerationError::Invalid(format!("durable storage: {other:?}")),
    }
}
impl ConditionalObjects for StorageObjects {
    fn get(&self, key: &str) -> Result<Option<ObjectBytes>> {
        let key = format!("{}/{key}", self.prefix);
        match self.runtime.block_on(
            self.storage
                .get_with_e_tag(&key, GetOptions::default().with_strong_consistency()),
        ) {
            Ok((bytes, Some(etag))) => Ok(Some(ObjectBytes {
                bytes: (*bytes).clone(),
                etag: etag.0,
            })),
            Ok((_, None)) => Err(GenerationError::Invalid("durable object lacks ETag".into())),
            Err(StorageError::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_error(e)),
        }
    }
    fn put(&self, key: &str, bytes: &[u8], expected: Option<&str>) -> Result<String> {
        let mode = expected
            .map(|s| PutMode::IfMatch(ETag(s.into())))
            .unwrap_or(PutMode::IfNotExist);
        self.runtime
            .block_on(self.storage.put_stream(
                &format!("{}/{key}", self.prefix),
                bytes.len(),
                futures::stream::iter([Ok(bytes.to_vec().into())]),
                PutOptions::default().with_mode(mode),
            ))
            .map_err(storage_error)?
            .map(|e| e.0)
            .ok_or_else(|| GenerationError::Invalid("conditional put lacks ETag".into()))
    }
}

#[cfg(test)]
pub(super) mod mock {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::BTreeMap;
    #[derive(Default)]
    pub struct MockObjects {
        pub objects: Mutex<BTreeMap<String, (Vec<u8>, String)>>,
        pub reads: Mutex<Vec<String>>,
        pub writes: Mutex<Vec<String>>,
        pub fail_prefix: Mutex<Option<String>>,
        pub fail_after_put: Mutex<bool>,
        pub latest_put_failure: Mutex<Option<(Option<Vec<u8>>, bool)>>,
        pub fail_latest_read: Mutex<bool>,
        pub cas_barrier: Mutex<Option<Arc<std::sync::Barrier>>>,
    }
    fn registry() -> &'static Mutex<BTreeMap<String, Arc<MockObjects>>> {
        static BACKENDS: OnceLock<Mutex<BTreeMap<String, Arc<MockObjects>>>> = OnceLock::new();
        BACKENDS.get_or_init(Mutex::default)
    }
    pub fn register() -> (DurableStoreConfig, Arc<MockObjects>) {
        let config = DurableStoreConfig {
            bucket: "sandbox-fixture".into(),
            prefix: format!("sha256/{}", uuid::Uuid::new_v4()),
        };
        let objects = Arc::new(MockObjects::default());
        registry()
            .lock()
            .insert(config.prefix.clone(), objects.clone());
        (config, objects)
    }
    pub fn lookup(config: &DurableStoreConfig) -> Option<Arc<dyn ConditionalObjects>> {
        registry()
            .lock()
            .get(&config.prefix)
            .cloned()
            .map(|b| b as Arc<dyn ConditionalObjects>)
    }
    impl ConditionalObjects for MockObjects {
        fn get(&self, key: &str) -> Result<Option<ObjectBytes>> {
            self.reads.lock().push(key.into());
            if key == "latest.json" && std::mem::take(&mut *self.fail_latest_read.lock()) {
                return Err(GenerationError::Invalid(
                    "injected reconciliation read failure".into(),
                ));
            }
            Ok(self
                .objects
                .lock()
                .get(key)
                .map(|(bytes, etag)| ObjectBytes {
                    bytes: bytes.clone(),
                    etag: etag.clone(),
                }))
        }
        fn put(&self, key: &str, bytes: &[u8], expected: Option<&str>) -> Result<String> {
            self.writes.lock().push(key.into());
            if self
                .fail_prefix
                .lock()
                .as_ref()
                .is_some_and(|p| key.starts_with(p))
            {
                return Err(GenerationError::Invalid(
                    "injected durable write failure".into(),
                ));
            }
            if key == "latest.json" {
                if let Some((replacement, fail_read)) = self.latest_put_failure.lock().take() {
                    if let Some(bytes) = replacement {
                        self.objects
                            .lock()
                            .insert(key.into(), (bytes, uuid::Uuid::new_v4().to_string()));
                    }
                    *self.fail_latest_read.lock() = fail_read;
                    return Err(GenerationError::Invalid(
                        "injected ambiguous conditional PUT".into(),
                    ));
                }
                let barrier = self.cas_barrier.lock().clone();
                if let Some(barrier) = barrier {
                    barrier.wait();
                }
            }
            let mut map = self.objects.lock();
            if map.get(key).map(|(_, etag)| etag.as_str()) != expected {
                return Err(GenerationError::Conflict);
            }
            let etag = uuid::Uuid::new_v4().to_string();
            map.insert(key.into(), (bytes.to_vec(), etag.clone()));
            if std::mem::take(&mut *self.fail_after_put.lock()) {
                return Err(GenerationError::Invalid(
                    "injected lost write response".into(),
                ));
            }
            Ok(etag)
        }
    }
}
#[cfg(test)]
#[path = "durable_generation_tests.rs"]
mod tests;
