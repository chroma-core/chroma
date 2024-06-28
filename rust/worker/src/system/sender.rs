use std::fmt::Debug;

use crate::errors::{ChromaError, ErrorCodes};

use super::{Component, ComponentContext, ComponentSender, Handler};
use async_trait::async_trait;
use thiserror::Error;

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
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> ();
}

#[async_trait]
impl<C, M> WrapperTrait<C> for Option<M>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        if let Some(message) = self.take() {
            component.handle(message, ctx).await;
        }
    }
}

pub(crate) fn wrap<C, M>(message: M, tracing_context: Option<tracing::Span>) -> Wrapper<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    Wrapper {
        wrapper: Box::new(Some(message)),
        tracing_context,
    }
}

// Receiver Traits

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

// Receiver Impls
#[derive(Debug)]
pub(super) struct ReceiverImpl<C>
where
    C: Component,
{
    pub(super) sender: ComponentSender<C>,
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
    pub(super) fn new(sender: ComponentSender<C>) -> Self {
        ReceiverImpl { sender }
    }
}

#[async_trait]
impl<C, M> ReceiverForMessage<M> for ReceiverImpl<C>
where
    C: Component + Handler<M>,
    M: Send + Debug + 'static,
{
    async fn send(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError> {
        // todo: is there a way to share these implementations?
        let res = self.sender.send(wrap(message, tracing_context)).await;
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
