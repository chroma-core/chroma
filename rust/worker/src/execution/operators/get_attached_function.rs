use async_trait::async_trait;
use chroma_error::ChromaError;
use chroma_sysdb::sysdb::SysDb;
use chroma_system::{Operator, OperatorType};
use chroma_types::{AttachedFunction, CollectionUuid, ListAttachedFunctionsError};
use thiserror::Error;

/// The `GetAttachedFunctionOperator` lists attached functions for a collection and selects the first one.
/// If no functions are found, it returns an empty result (not an error) to allow the orchestrator
/// to handle the case gracefully.
#[derive(Clone, Debug)]
pub struct GetAttachedFunctionOperator {
    pub sysdb: SysDb,
    pub collection_id: CollectionUuid,
}

impl GetAttachedFunctionOperator {
    pub fn new(sysdb: SysDb, collection_id: CollectionUuid) -> Self {
        Self {
            sysdb,
            collection_id,
        }
    }
}

#[derive(Debug)]
pub struct GetAttachedFunctionInput {
    pub collection_id: CollectionUuid,
}

#[derive(Debug)]
pub struct GetAttachedFunctionOutput {
    pub attached_function: Option<AttachedFunction>,
}

#[derive(Debug, Error)]
pub enum GetAttachedFunctionOperatorError {
    #[error("Failed to list attached functions: {0}")]
    ListFunctions(#[from] ListAttachedFunctionsError),
    #[error("No attached function found")]
    NoAttachedFunctionFound,
}

#[derive(Debug, Error)]
pub enum GetAttachedFunctionError {
    #[error("Failed to list attached functions: {0}")]
    ListFunctions(#[from] ListAttachedFunctionsError),
}

impl ChromaError for GetAttachedFunctionError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            GetAttachedFunctionError::ListFunctions(e) => e.code(),
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            GetAttachedFunctionError::ListFunctions(e) => e.should_trace_error(),
        }
    }
}

impl ChromaError for GetAttachedFunctionOperatorError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            GetAttachedFunctionOperatorError::ListFunctions(e) => e.code(),
            GetAttachedFunctionOperatorError::NoAttachedFunctionFound => {
                chroma_error::ErrorCodes::NotFound
            }
        }
    }

    fn should_trace_error(&self) -> bool {
        match self {
            GetAttachedFunctionOperatorError::ListFunctions(e) => e.should_trace_error(),
            GetAttachedFunctionOperatorError::NoAttachedFunctionFound => false,
        }
    }
}

#[async_trait]
impl Operator<GetAttachedFunctionInput, GetAttachedFunctionOutput> for GetAttachedFunctionOperator {
    type Error = GetAttachedFunctionOperatorError;

    fn get_type(&self) -> OperatorType {
        OperatorType::IO
    }

    async fn run(
        &self,
        input: &GetAttachedFunctionInput,
    ) -> Result<GetAttachedFunctionOutput, GetAttachedFunctionOperatorError> {
        tracing::trace!(
            "[{}]: Collection ID {}",
            self.get_name(),
            input.collection_id.0
        );

        let attached_functions = self
            .sysdb
            .clone()
            .get_attached_functions(chroma_sysdb::GetAttachedFunctionsOptions {
                input_collection_id: Some(input.collection_id),
                only_ready: true,
                ..Default::default()
            })
            .await?;

        if attached_functions.is_empty() {
            tracing::info!(
                "[{}]: No attached functions found for collection {}",
                self.get_name(),
                input.collection_id.0
            );
            return Ok(GetAttachedFunctionOutput {
                attached_function: None,
            });
        }

        // Take the first attached function from the list
        let attached_function = attached_functions
            .into_iter()
            .next()
            .ok_or(GetAttachedFunctionOperatorError::NoAttachedFunctionFound)?;

        tracing::info!(
            "[{}]: Found attached function '{}' for collection {}",
            self.get_name(),
            attached_function.name,
            input.collection_id.0
        );

        Ok(GetAttachedFunctionOutput {
            attached_function: Some(attached_function),
        })
    }
}
