use std::fmt::Debug;

use super::{Component, ComponentContext, Handler};
use async_trait::async_trait;
use thiserror::Error;

// Message Wrapper
#[derive(Debug)]
pub(crate) struct Wrapper<C>
where
    C: Component,
{
    wrapper: Box<dyn WrapperTrait<C>>,
}

impl<C: Component> Wrapper<C> {
    pub(super) async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        self.wrapper.handle(component, ctx).await;
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

pub(crate) fn wrap<C, M>(message: M) -> Wrapper<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    Wrapper {
        wrapper: Box::new(Some(message)),
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

    pub(crate) async fn send<M>(&self, message: M) -> Result<(), ChannelError>
    where
        C: Component + Handler<M>,
        M: Debug + Send + 'static,
    {
        let res = self.sender.send(wrap(message)).await;
        match res {
            Ok(_) => Ok(()),
            Err(_) => Err(ChannelError::SendError),
        }
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
pub(crate) trait Receiver<M>: Send + Sync + ReceiverClone<M> {
    async fn send(&self, message: M) -> Result<(), ChannelError>;
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
    async fn send(&self, message: M) -> Result<(), ChannelError> {
        let res = self.sender.send(wrap(message)).await;
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
