use super::{
    scheduler::Scheduler, system::System, Component, ComponentContext, ComponentSender,
    WrappedMessage,
};
use std::sync::Arc;
use tokio::{select, time::timeout};
use tracing::{Instrument, Span};

struct Inner<C>
where
    C: Component,
{
    pub(super) sender: ComponentSender<C>,
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
    queue_depth_metric: opentelemetry::metrics::Histogram<u64>,
}

impl<C> ComponentExecutor<C>
where
    C: Component + Send + 'static,
{
    pub(super) fn new(
        sender: ComponentSender<C>,
        cancellation_token: tokio_util::sync::CancellationToken,
        handler: C,
        system: System,
        scheduler: Scheduler,
    ) -> Self {
        let max_queue_depth = handler.queue_size();

        ComponentExecutor {
            inner: Arc::new(Inner {
                sender,
                cancellation_token,
                system,
                scheduler,
            }),
            handler,
            queue_depth_metric: opentelemetry::global::meter("chroma.execution.executor")
                .u64_histogram("component_queue_depth")
                .with_description("The depth of the component's message queue")
                .with_boundaries(
                    (0..=10)
                        .map(|i| (i * (max_queue_depth / 10)) as f64)
                        .collect::<Vec<_>>(),
                )
                .build(),
        }
    }

    pub(super) async fn run(
        &mut self,
        mut channel: tokio::sync::mpsc::Receiver<WrappedMessage<C>>,
    ) {
        self.handler
            .on_start(&ComponentContext {
                system: self.inner.system.clone(),
                sender: self.inner.sender.clone(),
                cancellation_token: self.inner.cancellation_token.clone(),
                scheduler: self.inner.scheduler.clone(),
            })
            .await;

        loop {
            let queue_depth = channel.max_capacity() - channel.capacity();
            self.queue_depth_metric.record(
                queue_depth as u64,
                &[opentelemetry::KeyValue::new(
                    "component_name",
                    C::get_name(),
                )],
            );

            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    if let Err(err) = timeout(
                        self.handler.on_stop_timeout(),
                        self.handler.on_stop(),
                    )
                    .await
                    {
                        tracing::error!("Unable to gracefully shutdown {:?}: {err}", self.handler);
                    }
                    break;
                }
                message = channel.recv() => {
                    match message {
                        Some(mut message) => {
                            let span: tracing::Span = message.get_tracing_context().unwrap_or(Span::current().clone());
                            let component_context = ComponentContext {
                                    system: self.inner.system.clone(),
                                    sender: self.inner.sender.clone(),
                                    cancellation_token: self.inner.cancellation_token.clone(),
                                    scheduler: self.inner.scheduler.clone(),
                            };
                            let task_future = message.handle(&mut self.handler, &component_context);
                            task_future.instrument(span).await;
                        }
                        None => {
                            tracing::error!("Channel closed");
                        }
                    }
                }
            }
        }
    }
}
