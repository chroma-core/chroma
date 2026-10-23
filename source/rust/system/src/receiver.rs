use super::{Component, ComponentSender, Handler, Message};
use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use parking_lot::Mutex;
use std::{fmt::Debug, sync::Arc};
use thiserror::Error;
use tokio::sync::oneshot;

/// A ReceiverForMessage is generic over a message type, and useful if you want to send a given message type to any component that can handle it.
#[async_trait]
pub trait ReceiverForMessage<M>: Send + Sync + Debug + ReceiverForMessageClone<M> {
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>;
}

pub trait ReceiverForMessageClone<M> {
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

#[derive(Debug)]
pub struct OneshotMessageReceiver<M> {
    sender: Arc<Mutex<Option<oneshot::Sender<M>>>>,
}

impl<M> OneshotMessageReceiver<M> {
    pub fn new() -> (Self, oneshot::Receiver<M>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                sender: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }
}

impl<M> Clone for OneshotMessageReceiver<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
        }
    }
}

#[async_trait]
impl<M> ReceiverForMessage<M> for OneshotMessageReceiver<M>
where
    M: Message,
{
    async fn send(
        &self,
        message: M,
        _tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        let sender = self
            .sender
            .lock()
            .take()
            .ok_or_else(|| ChannelError::SendError("Receiver already used".to_string()))?;
        sender
            .send(message)
            .map_err(|_| ChannelError::SendError("Failed to send message".to_string()))?;
        Ok(())
    }
}

// Errors
#[derive(Error, Debug)]
pub enum ChannelError {
    #[error("Failed to send message: {0}")]
    SendError(String),
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
}

impl ChromaError for RequestError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}
