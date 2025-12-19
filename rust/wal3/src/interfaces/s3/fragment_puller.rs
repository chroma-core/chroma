use std::sync::Arc;

use chroma_storage::Storage;

use crate::interfaces::FragmentConsumer;
use crate::{FragmentSeqNo, LogPosition, LogReaderOptions};

// TODO(rescrv):  Remove annotation.
#[allow(dead_code)]
pub struct FragmentPuller {
    options: LogReaderOptions,
    storage: Arc<Storage>,
    prefix: String,
}

impl FragmentPuller {
    pub fn new(options: LogReaderOptions, storage: Arc<Storage>, prefix: String) -> Self {
        Self {
            options,
            storage,
            prefix,
        }
    }
}

impl FragmentConsumer for FragmentPuller {
    type FragmentPointer = (FragmentSeqNo, LogPosition);
}
