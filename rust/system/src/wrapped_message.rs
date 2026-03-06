use super::{Component, ComponentContext, Handler, Message};
use async_trait::async_trait;
use futures::FutureExt;
use std::{fmt::Debug, panic::AssertUnwindSafe};
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
impl<C, M> HandleableMessage<C> for Option<HandleableMessageImpl<M, C::Result>>
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
                        if let Err(e) = reply_channel.send(result) {
                            tracing::error!(
                                "message reply channel was unexpectedly dropped by caller: {:?}",
                                e
                            );
                        }
                    }
                }
                Err(panic_value) => {
                    tracing::error!("Panic occurred while handling message: {:?}", panic_value);
                    component.on_handler_panic(panic_value);
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ComponentSender;

    #[tokio::test]
    async fn test_dropped_reply_channel_does_not_panic() {
        #[derive(Debug)]
        struct TestMessage;

        #[derive(Debug)]
        struct TestComponent;
        impl Component for TestComponent {
            fn get_name() -> &'static str {
                "TestComponent"
            }
            fn queue_size(&self) -> usize {
                10
            }
            fn on_handler_panic(&mut self, panic_value: Box<dyn std::any::Any + Send>) {
                tracing::error!("Handler panicked: {:?}", panic_value);
                std::panic::resume_unwind(panic_value);
            }
        }

        #[async_trait]
        impl Handler<TestMessage> for TestComponent {
            type Result = ();
            async fn handle(&mut self, _: TestMessage, _: &ComponentContext<Self>) -> () {}
        }

        let system = crate::System::new();

        let mut comp = TestComponent;
        let (tx, _) = tokio::sync::mpsc::channel(comp.queue_size());
        let sender = ComponentSender::new(tx);

        let ctx = ComponentContext {
            system,
            sender,
            cancellation_token: tokio_util::sync::CancellationToken::new(),
            scheduler: crate::scheduler::Scheduler::new(),
        };
        let (tx, rx) = oneshot::channel();
        let mut msg = Some(HandleableMessageImpl::new(TestMessage, Some(tx)));

        drop(rx); // simulates a receiver disconnecting, nothing left to handle the message
        msg.handle_and_reply(&mut comp, &ctx).await;

        // done if doesn't panic
    }
}
