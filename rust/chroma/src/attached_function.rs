//! Attached function handle for interacting with function attachments on collections.

use std::sync::Arc;

use chroma_types::{AttachedFunctionApiResponse, CollectionUuid};

use crate::{client::ChromaHttpClientError, ChromaHttpClient};

/// A handle to an attached function bound to a specific input collection.
///
/// This type represents one attached-function instance as viewed from a particular
/// input collection. For multi-input async attached functions, calling
/// [`add_input`](Self::add_input) returns another handle scoped to the newly added
/// input collection.
#[derive(Clone)]
pub struct ChromaAttachedFunction {
    pub(crate) client: ChromaHttpClient,
    pub(crate) attached_function: Arc<AttachedFunctionApiResponse>,
}

impl std::fmt::Debug for ChromaAttachedFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChromaAttachedFunction")
            .field("id", &self.attached_function.id)
            .field("name", &self.attached_function.name)
            .field("function_name", &self.attached_function.function_name)
            .field(
                "input_collection_id",
                &self.attached_function.input_collection_id,
            )
            .field(
                "output_collection",
                &self.attached_function.output_collection_name,
            )
            .finish()
    }
}

impl ChromaAttachedFunction {
    /// Returns the unique identifier of the attached function instance.
    pub fn id(&self) -> chroma_types::AttachedFunctionUuid {
        self.attached_function.id
    }

    /// Returns the user-assigned name of the attached function instance.
    pub fn name(&self) -> &str {
        &self.attached_function.name
    }

    /// Returns the built-in function name.
    pub fn function_name(&self) -> &str {
        &self.attached_function.function_name
    }

    /// Returns the input collection currently associated with this handle.
    pub fn input_collection_id(&self) -> CollectionUuid {
        self.attached_function.input_collection_id
    }

    /// Returns the output collection name.
    pub fn output_collection(&self) -> &str {
        &self.attached_function.output_collection_name
    }

    /// Adds a new input collection to this attached function.
    ///
    /// The server enforces that only async attached functions may have multiple
    /// input collections. This method is idempotent: if the input is already
    /// attached, the existing association is returned.
    pub async fn add_input(
        &self,
        input_collection_id: CollectionUuid,
    ) -> Result<Self, ChromaHttpClientError> {
        self.client
            .add_attached_function_input(
                self.attached_function.input_collection_id,
                self.attached_function.name.clone(),
                input_collection_id,
            )
            .await
    }
}
