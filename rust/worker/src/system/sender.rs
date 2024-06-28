use std::fmt::Debug;

use crate::errors::{ChromaError, ErrorCodes};

use super::{Component, ComponentContext, Handler};
use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::oneshot;

type AnyMessageResult = Box<dyn Debug + Send>;

// Message Wrapper
#[derive(Debug)]
pub(crate) struct Wrapper<C>
where
    C: Component,
{
    // todo: rename
    wrapper: Box<dyn WrapperTrait<C>>,
    // todo: limit pub scope?
    // pub response_tx: Box<dyn ResultReplyTrait<C>>, //Option<oneshot::Sender<AnyMessageResult>>,
    tracing_context: Option<tracing::Span>,
}

impl<C: Component> Wrapper<C> {
    pub(super) async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) {
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
struct MessageWithReplyChannel<M: Debug + Send + 'static, Result: Send> {
    message: M,
    reply_channel: oneshot::Sender<Result>,
}

impl<M: Debug + Send + 'static, Result: Send> MessageWithReplyChannel<M, Result> {
    fn new(message: M, reply_channel: oneshot::Sender<Result>) -> Self {
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
            message.reply_channel.send(result).unwrap();
        }
    }
}

pub(crate) fn wrap<C, M>(
    message: MessageWithReplyChannel<M, C::Result>,
    tracing_context: Option<tracing::Span>,
) -> Wrapper<C>
where
    // todo
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
        // todo
        C: Component + Handler<M>,
        M: Debug + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let message_with_reply_channel = MessageWithReplyChannel::new(message, tx);
        println!("sending message...");
        let res = self
            .sender
            .send(wrap(message_with_reply_channel, tracing_context))
            .await;
        println!("waiting for result...");
        let result = rx.await;
        println!("got result in sender: {:?}", result);
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
    // todo
    C: Component + Handler<M>,
    M: Send + Debug + 'static,
{
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        let (tx, rx) = oneshot::channel();
        let message_with_reply_channel = MessageWithReplyChannel::new(message, tx);

        println!("sending message... {:?}", message_with_reply_channel);

        let res = self
            .sender
            .send(wrap(message_with_reply_channel, tracing_context))
            .await;

        let result = rx.await.unwrap();
        println!("got result in receiver: {:?}", result);
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::SendError),
        }
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
