use super::{Component, ComponentContext, Handler};
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::sync::oneshot;

// Why is this separate from the WrappedMessage struct? The size of WrappedMessage
// must be constant for all messages on a given component, so the alternative would
// be to add a field to WrappedMessage like Box<dyn ReplyableMessage<C>>
// (similar to how the outgoing message is boxed). Unfortunately this doesn't work
// as the message generic (M) would have to be declared at the method level and this
// is incompatible with dynamic dispatch.
// (https://doc.rust-lang.org/error_codes/E0038.html#method-has-generic-type-parameters)
#[derive(Debug)]
pub(crate) struct MessageWithReplyChannel<M: Debug + Send + 'static, Result: Send> {
    message: M,
    // Optional because not all messages require a reply, .send() does not provide a reply channel but .request() does.
    reply_channel: Option<oneshot::Sender<Result>>,
}

impl<M: Debug + Send + 'static, Result: Send> MessageWithReplyChannel<M, Result> {
    pub(super) fn new(message: M, reply_channel: Option<oneshot::Sender<Result>>) -> Self {
        MessageWithReplyChannel {
            message,
            reply_channel,
        }
    }
}

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
    pub(super) fn new<M>(
        message: M,
        reply_channel: Option<oneshot::Sender<C::Result>>,
        tracing_context: Option<tracing::Span>,
    ) -> Self
    where
        C: Component + Handler<M>,
        M: Debug + Send + 'static,
    {
        WrappedMessage {
            boxed_message: Box::new(Some(MessageWithReplyChannel::new(message, reply_channel))),
            tracing_context,
        }
    }

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
impl<C, M> HandleableMessage<C> for Option<MessageWithReplyChannel<M, C::Result>>
where
    C: Component + Handler<M>,
    M: Debug + Send + 'static,
{
    async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        if let Some(message) = self.take() {
            let result = component.handle(message.message, ctx).await;
            if let Some(reply_channel) = message.reply_channel {
                reply_channel.send(result).unwrap();
            }
        }
    }
}
