use super::{
    scheduler::Scheduler,
    sender::{Sender, Wrapper},
    system::System,
    Component,
};
use crate::system::ComponentContext;
use std::sync::Arc;
use tokio::select;
use tracing::{trace_span, Instrument, Span};

struct Inner<C>
where
    C: Component,
{
    pub(super) sender: Sender<C>,
    pub(super) cancellation_token: tokio_util::sync::CancellationToken,
    pub(super) system: System,
    pub(super) scheduler: Scheduler,
}

#[derive(Clone)]
/// # Description
/// The executor holds the context for a components execution and is responsible for
/// running the components handler methods
pub(super) struct ComponentExecutor<C>
where
    C: Component,
{
    inner: Arc<Inner<C>>,
    handler: C,
}

impl<C> ComponentExecutor<C>
where
    C: Component + Send + 'static,
{
    pub(super) fn new(
        sender: Sender<C>,
        cancellation_token: tokio_util::sync::CancellationToken,
        handler: C,
        system: System,
        scheduler: Scheduler,
    ) -> Self {
        ComponentExecutor {
            inner: Arc::new(Inner {
                sender,
                cancellation_token,
                system,
                scheduler,
            }),
            handler,
        }
    }

    pub(super) async fn run(&mut self, mut channel: tokio::sync::mpsc::Receiver<Wrapper<C>>) {
        self.handler
            .on_start(&ComponentContext {
                system: self.inner.system.clone(),
                sender: self.inner.sender.clone(),
                cancellation_token: self.inner.cancellation_token.clone(),
                scheduler: self.inner.scheduler.clone(),
            })
            .await;
        loop {
            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    break;
                }
                message = channel.recv() => {
                    match message {
                        Some(mut message) => {
                            let parent_span: tracing::Span;
                            match message.get_tracing_context() {
                                Some(spn) => {
                                    parent_span = spn;
                                },
                                None => {
                                    parent_span = Span::current().clone();
                                }
                            }
                            let child_span = trace_span!(parent: parent_span, "task handler");
                            let component_context = ComponentContext {
                                    system: self.inner.system.clone(),
                                    sender: self.inner.sender.clone(),
                                    cancellation_token: self.inner.cancellation_token.clone(),
                                    scheduler: self.inner.scheduler.clone(),
                            };
                            let task_future = message.handle(&mut self.handler, &component_context);
                            let result = task_future.instrument(child_span).await;
                            println!("got result in executor: {:?}", result);
                            let tx = message.response_tx.unwrap();
                            tx.send(result).unwrap();
                        }
                        None => {
                            // TODO: Log error
                        }
                    }
                }
            }
        }
    }
}
