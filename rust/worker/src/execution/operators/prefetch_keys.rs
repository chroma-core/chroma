use async_trait::async_trait;
use chroma_blockstore::{
    arrow::types::{ArrowReadableKey, ArrowReadableValue},
    key::{InvalidKeyConversion, KeyWrapper},
    memory::Readable,
    provider::{BlockfileProvider, OpenError},
    BlockfileReader, Key, Value,
};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::{Operator, OperatorType};
use futures::FutureExt;
use parking_lot::Mutex;
use std::{future::Future, pin::Pin};
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum PrefetchKeysError {
    #[error("Error constructing reader: {0}")]
    CouldNotConstructReader(#[from] Box<OpenError>),
    #[error("Expected future missing. Was the operator ran twice with the same input?")]
    MissingFuture,
}

impl ChromaError for PrefetchKeysError {
    fn code(&self) -> ErrorCodes {
        match self {
            PrefetchKeysError::CouldNotConstructReader(e) => e.code(),
            PrefetchKeysError::MissingFuture => ErrorCodes::Internal,
        }
    }
}

pub struct PrefetchKeysInput {
    #[allow(clippy::type_complexity)]
    prefetch_fut:
        Mutex<Option<Pin<Box<dyn Future<Output = Result<(), PrefetchKeysError>> + Send>>>>,
}

impl std::fmt::Debug for PrefetchKeysInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchKeysInput").finish()
    }
}

impl PrefetchKeysInput {
    pub fn new<
        'referred_data,
        K: Key
            + Into<KeyWrapper>
            + TryFrom<&'referred_data KeyWrapper, Error = InvalidKeyConversion>
            + ArrowReadableKey<'referred_data>
            + Sync
            + Send
            + 'referred_data + 'static,
        V: Value
            + Readable<'referred_data>
            + ArrowReadableValue<'referred_data>
            + Sync
            + Send
            + 'referred_data,
    >(
        blockfile_provider: BlockfileProvider,
        blockfile_id: Uuid,
        keys: Vec<(String, K)>,
    ) -> PrefetchKeysInput {
        // This future will not do any work until awaited. We construct it here so we can erase the generics.
        let prefetch_fut = async move {
            let reader: BlockfileReader<'referred_data, K, V> =
                blockfile_provider.read(&blockfile_id).await?;
            reader.load_blocks_for_keys(keys).await;
            Ok(())
        };

        PrefetchKeysInput {
            prefetch_fut: Mutex::new(Some(prefetch_fut.boxed())),
        }
    }
}

pub type PrefetchKeysOutput = ();

#[derive(Debug, Default)]
pub struct PrefetchKeysOperator;

impl PrefetchKeysOperator {
    pub fn new() -> PrefetchKeysOperator {
        PrefetchKeysOperator {}
    }
}

#[async_trait]
impl Operator<PrefetchKeysInput, PrefetchKeysOutput> for PrefetchKeysOperator {
    type Error = PrefetchKeysError;

    async fn run(
        &self,
        input: &PrefetchKeysInput,
    ) -> Result<PrefetchKeysOutput, PrefetchKeysError> {
        // .into_future() (implicitly called) consumes self, so we need to take it out of the input
        let fut = input
            .prefetch_fut
            .lock()
            .take()
            .ok_or(PrefetchKeysError::MissingFuture)?;
        fut.await?;

        Ok(())
    }

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }
}
