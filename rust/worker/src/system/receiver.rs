use super::{Component, ComponentSender, Handler, Message};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use std::fmt::Debug;
use thiserror::Error;

/// A ReceiverForMessage is generic over a message type, and useful if you want to send a given message type to any component that can handle it.
#[async_trait]
pub(crate) trait ReceiverForMessage<M>:
    Send + Sync + Debug + ReceiverForMessageClone<M>
{
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>;
}

pub(crate) trait ReceiverForMessageClone<M> {
    fn clone_box(&self) -> Box<dyn ReceiverForMessage<M>>;
}

impl<M> Clone for Box<dyn ReceiverForMessage<M>> {
    fn clone(&self) -> Box<dyn ReceiverForMessage<M>> {
        self.clone_box()
    }
}

impl<T, M> ReceiverForMessageClone<M> for T
where
    T: 'static + ReceiverForMessage<M> + Clone,
{
    fn clone_box(&self) -> Box<dyn ReceiverForMessage<M>> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl<C, M> ReceiverForMessage<M> for ComponentSender<C>
where
    C: Component + Handler<M>,
    M: Message,
{
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        self.wrap_and_send(message, tracing_context).await
    }
}

// Errors
#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("Failed to send message")]
    SendError,
}

impl ChromaError for ChannelError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Error, Debug, PartialEq)]
#[allow(dead_code)]
pub enum RequestError {
    #[error("Failed to send request")]
    SendError,
    #[error("Failed to receive response")]
    ReceiveError,
    #[error("Message handler panicked")]
    HandlerPanic(Option<String>),
}

impl ChromaError for RequestError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}
