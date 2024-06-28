use std::fmt::Debug;

use crate::errors::{ChromaError, ErrorCodes};

use super::{Component, ComponentContext, Handler};
use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::oneshot;

// Message Wrapper
#[derive(Debug)]
pub(crate) struct Wrapper<C>
where
    C: Component,
{
    wrapper: Box<dyn WrapperTrait<C>>,
    tracing_context: Option<tracing::Span>,
}

impl<C: Component> Wrapper<C> {
    pub(super) async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        self.wrapper.handle(component, ctx).await;
    }

    pub(super) fn get_tracing_context(&self) -> Option<tracing::Span> {
        return self.tracing_context.clone();
    }
}

#[async_trait]
pub(super) trait WrapperTrait<C>: Debug + Send
where
    C: Component,
{
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>);
}

#[derive(Debug)]
pub(crate) struct MessageWithReplyChannel<M: Debug + Send + 'static, Result: Send> {
    message: M,
    // Optional because not all messages require a reply, .send() does not provide a reply channel but .request() does.
    reply_channel: Option<oneshot::Sender<Result>>,
}

impl<M: Debug + Send + 'static, Result: Send> MessageWithReplyChannel<M, Result> {
    fn new(message: M, reply_channel: Option<oneshot::Sender<Result>>) -> Self {
        MessageWithReplyChannel {
            message,
            reply_channel,
        }
    }
}

#[async_trait]
impl<C, M> WrapperTrait<C> for Option<MessageWithReplyChannel<M, C::Result>>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) {
        if let Some(message) = self.take() {
            let result = component.handle(message.message, ctx).await;
            if let Some(reply_channel) = message.reply_channel {
                // todo: avoid unwrap?
                reply_channel.send(result).unwrap();
            }
        }
    }
}

pub(crate) fn wrap<C, M>(
    message: MessageWithReplyChannel<M, C::Result>,
    tracing_context: Option<tracing::Span>,
) -> Wrapper<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    Wrapper {
        wrapper: Box::new(Some(message)),
        tracing_context,
    }
}

// Sender
pub(crate) struct Sender<C>
where
    C: Component + Send + 'static,
{
    pub(super) sender: tokio::sync::mpsc::Sender<Wrapper<C>>,
}

impl<C> Sender<C>
where
    C: Component + Send + 'static,
{
    pub(super) fn new(sender: tokio::sync::mpsc::Sender<Wrapper<C>>) -> Self {
        Sender { sender }
    }

    pub(crate) async fn send<M>(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>
    where
        C: Component + Handler<M>,
        M: Debug + Send + 'static,
    {
        let message_with_reply_channel = MessageWithReplyChannel::new(message, None);
        let res = self
            .sender
            .send(wrap(message_with_reply_channel, tracing_context))
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::SendError),
        }
    }

    pub(crate) fn as_receiver<M>(&self) -> Box<dyn Receiver<M>>
    where
        C: Component + Handler<M>,
        M: Debug + Send + 'static,
    {
        Box::new(ReceiverImpl::new(self.sender.clone()))
    }
}

impl<C> Clone for Sender<C>
where
    C: Component,
{
    fn clone(&self) -> Self {
        Sender {
            sender: self.sender.clone(),
        }
    }
}

// Reciever Traits

#[async_trait]
pub(crate) trait Receiver<M>: Send + Sync + Debug + ReceiverClone<M> {
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>;
}

trait ReceiverClone<M> {
    fn clone_box(&self) -> Box<dyn Receiver<M>>;
}

#[derive(Error, Debug)]
pub(crate) enum RequestError {
    #[error("failed to send message")]
    SendError,
    #[error("failed to receive result")]
    ReceiveError,
}

#[async_trait]
pub(crate) trait RequestableReceiver<C, M>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    async fn request(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<C::Result, RequestError>;
}

pub(crate) trait AllReceiver<C, M>: Receiver<M> + RequestableReceiver<C, M>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
}

impl<M> Clone for Box<dyn Receiver<M>> {
    fn clone(&self) -> Box<dyn Receiver<M>> {
        self.clone_box()
    }
}

impl<T, M> ReceiverClone<M> for T
where
    T: 'static + Receiver<M> + Clone,
{
    fn clone_box(&self) -> Box<dyn Receiver<M>> {
        Box::new(self.clone())
    }
}

// Reciever Impls
#[derive(Debug)]
pub(super) struct ReceiverImpl<C>
where
    C: Component,
{
    pub(super) sender: tokio::sync::mpsc::Sender<Wrapper<C>>,
}

impl<C> Clone for ReceiverImpl<C>
where
    C: Component,
{
    fn clone(&self) -> Self {
        ReceiverImpl {
            sender: self.sender.clone(),
        }
    }
}

impl<C> ReceiverImpl<C>
where
    C: Component,
{
    pub(super) fn new(sender: tokio::sync::mpsc::Sender<Wrapper<C>>) -> Self {
        ReceiverImpl { sender }
    }
}

#[async_trait]
impl<C, M> Receiver<M> for ReceiverImpl<C>
where
    C: Component + Handler<M>,
    M: Send + Debug + 'static,
{
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        let message_with_reply_channel = MessageWithReplyChannel::new(message, None);

        let res = self
            .sender
            .send(wrap(message_with_reply_channel, tracing_context))
            .await;

        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::SendError),
        }
    }
}

#[async_trait]
impl<C, M> RequestableReceiver<C, M> for ReceiverImpl<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    async fn request(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<C::Result, RequestError> {
        let (tx, rx) = oneshot::channel();
        let message_with_reply_channel = MessageWithReplyChannel::new(message, Some(tx));

        self.sender
            .send(wrap(message_with_reply_channel, tracing_context))
            .await
            .map_err(|_| RequestError::SendError)?;

        let result = rx.await.map_err(|_| RequestError::ReceiveError)?;
        Ok(result)
    }
}

#[async_trait]
impl<C, M> AllReceiver<C, M> for ReceiverImpl<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
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
