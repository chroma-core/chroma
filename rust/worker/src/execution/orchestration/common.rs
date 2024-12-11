use crate::system::{Component, ComponentContext};
use chroma_error::ChromaError;

/// Terminate the orchestrator with an error
/// This function sends an error to the result channel and cancels the orchestrator
/// so it stops processing
/// # Arguments
/// * `result_channel` - The result channel to send the error to
/// * `error` - The error to send
/// * `ctx` - The component context
/// # Panics
/// This function panics if the result channel is not set
pub(super) fn terminate_with_error<Output, C, E>(
    mut result_channel: Option<tokio::sync::oneshot::Sender<Result<Output, E>>>,
    error: E,
    ctx: &ComponentContext<C>,
) where
    C: Component,
    E: ChromaError,
{
    let result_channel = result_channel
        .take()
        .expect("Invariant violation. Result channel is not set.");
    match result_channel.send(Err(error)) {
        Ok(_) => (),
        Err(_) => {
            tracing::error!("Result channel dropped before sending error");
        }
    }
    // Cancel the orchestrator so it stops processing
    ctx.cancellation_token.cancel();
}
