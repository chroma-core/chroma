//! This is an admission controller suitable for limiting the number of outstanding requests to object storage.

use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMultipartOpts,
    PutOptions, PutPayload, PutResult, Result,
};
use sync42::state_hash_table::{Key, StateHashTable, Value};

pub enum AdmissionControlOutcome<'a> {
    Semaphore(tokio::sync::SemaphorePermit<'a>),
    Reject,
}

pub trait AdmissionControlPolicy: Debug + Send + Sync {
    fn new_enforcer(&self) -> Arc<dyn AdmissionControlEnforcer + 'static>;
}

#[async_trait::async_trait]
pub trait AdmissionControlEnforcer: Debug + Send + Sync {
    async fn enter<'a>(&'a self) -> AdmissionControlOutcome<'a>;
}

#[derive(Debug)]
pub struct CountBasedAdmissionControlPolicy {
    max_concurrent_requests: usize,
}

impl AdmissionControlPolicy for CountBasedAdmissionControlPolicy {
    fn new_enforcer(&self) -> Arc<dyn AdmissionControlEnforcer + 'static> {
        Arc::new(CountBasedAdmissionControlEnforcer {
            semaphore: tokio::sync::Semaphore::new(self.max_concurrent_requests),
        })
    }
}

#[derive(Debug)]
pub struct CountBasedAdmissionControlEnforcer {
    semaphore: tokio::sync::Semaphore,
}

#[async_trait::async_trait]
impl AdmissionControlEnforcer for CountBasedAdmissionControlEnforcer {
    async fn enter<'a>(&'a self) -> AdmissionControlOutcome<'a> {
        let permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            Err(err) => {
                tracing::error!("semaphore acquire failed: {:?}", err);
                return AdmissionControlOutcome::Reject;
            }
        };
        AdmissionControlOutcome::Semaphore(permit)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct PrefixAdmissionControlKey {
    path: String,
}

impl Key for PrefixAdmissionControlKey {}

#[derive(Debug)]
struct PrefixAdmissionControlValue {
    policy: Mutex<Option<Arc<dyn AdmissionControlEnforcer>>>,
}

impl Default for PrefixAdmissionControlValue {
    fn default() -> Self {
        Self {
            policy: Mutex::new(None),
        }
    }
}

impl Value for PrefixAdmissionControlValue {
    fn finished(&self) -> bool {
        // This method is consulted only when someone is the last referent of the state, so return
        // true always.  It will only be considered finished when this returns true and someone is
        // able to garbage collect the state.
        true
    }
}

impl From<PrefixAdmissionControlKey> for PrefixAdmissionControlValue {
    fn from(_: PrefixAdmissionControlKey) -> Self {
        Self::default()
    }
}

pub struct AdmissionControlledObjectStore {
    policy: Arc<dyn AdmissionControlPolicy>,
    object_store: Arc<dyn ObjectStore>,
    prefix_control: StateHashTable<PrefixAdmissionControlKey, PrefixAdmissionControlValue>,
}

impl Debug for AdmissionControlledObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionControlledObjectStore")
            .field("object_store", &"Arc<dyn ObjectStore>")
            .finish()
    }
}

impl Display for AdmissionControlledObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AdmissionControlledObjectStore")
    }
}

macro_rules! wrap_operation {
    ($this:ident, $location:expr, $op:expr) => {{
        let location = $location.to_string();
        let (location, _) = location.rsplit_once('/').unwrap_or((location.as_str(), ""));
        let prefix_state = $this
            .prefix_control
            .get_or_create_state(PrefixAdmissionControlKey {
                path: location.to_string(),
            });
        let policy = {
            // SAFETY(rescrv):  Mutex poisoning.
            let mut policy = prefix_state.policy.lock().unwrap();
            if let Some(policy) = &*policy {
                Arc::clone(policy)
            } else {
                let p = $this.policy.new_enforcer();
                *policy = Some(Arc::clone(&p));
                p
            }
        };
        let ticket = policy.enter();
        let result = $op;
        drop(ticket);
        result
    }};
}

#[async_trait::async_trait]
impl ObjectStore for AdmissionControlledObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.put_opts(location, payload, opts).await
        )
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.put_multipart_opts(location, opts).await
        )
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.get_opts(location, options).await
        )
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<usize>]) -> Result<Vec<Bytes>> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.get_ranges(location, ranges).await
        )
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.head(location).await
        )
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        wrap_operation!(
            self,
            location.clone(),
            self.object_store.delete(location).await
        )
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        wrap_operation!(
            self,
            prefix.unwrap_or(&Path::from(".")).clone(),
            self.object_store.list(prefix)
        )
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        wrap_operation!(
            self,
            prefix.unwrap_or(&Path::from(".")).clone(),
            self.object_store.list_with_delimiter(prefix).await
        )
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        wrap_operation!(self, from.clone(), self.object_store.copy(from, to).await)
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        wrap_operation!(
            self,
            from.clone(),
            self.object_store.copy_if_not_exists(from, to).await
        )
    }
}
