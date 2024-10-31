use crate::utils::get_panic_message;

use super::{Component, ComponentContext, Handler, Message};
use async_trait::async_trait;
use futures::FutureExt;
use std::{fmt::Debug, panic::AssertUnwindSafe};
use thiserror::Error;
use tokio::sync::oneshot;

// Why is this separate from the WrappedMessage struct? WrappedMessage is only generic
// over the component type, not the message type—but the reply channel must be generic
// over both the component and message types so that it can obtain the result type.
// And declaring the message generic (M) at the method level is incompatible with dynamic dispatch.
// (https://doc.rust-lang.org/error_codes/E0038.html#method-has-generic-type-parameters)
#[derive(Debug)]
pub(crate) struct HandleableMessageImpl<M: Message, Result: Send> {
    message: M,
    // Optional because not all messages require a reply, .send() does not provide a reply channel but .request() does.
    reply_channel: Option<oneshot::Sender<Result>>,
}

impl<M: Message, Result: Send> HandleableMessageImpl<M, Result> {
    pub(super) fn new(message: M, reply_channel: Option<oneshot::Sender<Result>>) -> Self {
        HandleableMessageImpl {
            message,
            reply_channel,
        }
    }
}

#[derive(Debug, Error)]
pub(super) enum MessageHandlerError {
    #[error("Panic occurred while handling message: {0:?}")]
    Panic(Option<String>),
}

type MessageHandlerWrappedResult<R> = Result<R, MessageHandlerError>;

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
        reply_channel: Option<oneshot::Sender<MessageHandlerWrappedResult<C::Result>>>,
        tracing_context: Option<tracing::Span>,
    ) -> Self
    where
        C: Component + Handler<M>,
        M: Message,
    {
        WrappedMessage {
            boxed_message: Box::new(Some(HandleableMessageImpl::new(message, reply_channel))),
            tracing_context,
        }
    }

    pub(super) async fn handle(&mut self, component: &mut C, ctx: &ComponentContext<C>) {
        self.boxed_message.handle_and_reply(component, ctx).await;
    }

    pub(super) fn get_tracing_context(&self) -> Option<tracing::Span> {
        self.tracing_context.clone()
    }
}

#[async_trait]
pub(super) trait HandleableMessage<C>: Debug + Send
where
    C: Component,
{
    async fn handle_and_reply(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> ();
}

#[async_trait]
impl<C, M> HandleableMessage<C>
    for Option<HandleableMessageImpl<M, MessageHandlerWrappedResult<C::Result>>>
where
    C: Component + Handler<M>,
    M: Message,
{
    async fn handle_and_reply(&mut self, component: &mut C, ctx: &ComponentContext<C>) -> () {
        if let Some(message) = self.take() {
            let result = AssertUnwindSafe(component.handle(message.message, ctx))
                .catch_unwind()
                .await;

            match result {
                Ok(result) => {
                    if let Some(reply_channel) = message.reply_channel {
                        reply_channel
                            .send(Ok(result))
                            .expect("message reply channel was unexpectedly dropped by caller");
                    }
                }
                Err(panic_value) => {
                    let panic_message = get_panic_message(panic_value);

                    if let Some(reply_channel) = message.reply_channel {
                        reply_channel
                            .send(Err(MessageHandlerError::Panic(panic_message)))
                            .expect("message reply channel was unexpectedly dropped by caller");
                    }
                }
            };
        }
    }
}
