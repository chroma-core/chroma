use super::{Component, ComponentContext, Handler};
use async_trait::async_trait;
use std::fmt::Debug;

/// Erases the type of the message so it can be sent over a channel and optionally bundles a tracing context.
#[derive(Debug)]
pub(crate) struct WrappedMessage<C>
where
    C: Component,
{
    boxed_message: Box<dyn HandleableMessage<C>>,
    tracing_context: Option<tracing::Span>,
}

impl<C: Component> WrappedMessage<C> {
    pub(super) async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        self.boxed_message.handle(component, ctx).await;
    }

    pub(super) fn get_tracing_context(&self) -> Option<tracing::Span> {
        return self.tracing_context.clone();
    }
}

#[async_trait]
pub(super) trait HandleableMessage<C>: Debug + Send
where
    C: Component,
{
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> ();
}

#[async_trait]
impl<C, M> HandleableMessage<C> for Option<M>
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

pub(crate) fn wrap<C, M>(message: M, tracing_context: Option<tracing::Span>) -> WrappedMessage<C>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    WrappedMessage {
        boxed_message: Box::new(Some(message)),
        tracing_context,
    }
}
