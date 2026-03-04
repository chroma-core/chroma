use super::{
    scheduler::Scheduler, system::System, utils::duration_ms, Component, ComponentContext,
    ComponentSender, WrappedMessage,
};
use crate::types::ComponentRuntimeStats;
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
    metrics: ComponentExecutorMetrics,
    runtime_stats: Arc<ComponentRuntimeStats>,
}

#[derive(Clone)]
struct ComponentExecutorMetrics {
    queue_depth: opentelemetry::metrics::Histogram<u64>,
    message_received_total: opentelemetry::metrics::Counter<u64>,
    handler_completed_total: opentelemetry::metrics::Counter<u64>,
    handler_latency_ms: opentelemetry::metrics::Histogram<f64>,
    stop_total: opentelemetry::metrics::Counter<u64>,
    on_stop_latency_ms: opentelemetry::metrics::Histogram<f64>,
}

impl ComponentExecutorMetrics {
    fn new(max_queue_depth: usize) -> Self {
        let meter = opentelemetry::global::meter("chroma.system");
        Self {
            queue_depth: meter
                .u64_histogram("chroma.system.executor.queue_depth")
                .with_description("The depth of the component's message queue")
                .with_boundaries(
                    (0..=10)
                        .map(|i| (i * (max_queue_depth / 10)) as f64)
                        .collect::<Vec<_>>(),
                )
                .build(),
            message_received_total: meter
                .u64_counter("chroma.system.executor.message_received_total")
                .with_description("Messages received by component executors")
                .build(),
            handler_completed_total: meter
                .u64_counter("chroma.system.executor.handler_completed_total")
                .with_description("Completed handler executions")
                .build(),
            handler_latency_ms: meter
                .f64_histogram("chroma.system.executor.handler_latency_ms")
                .with_description("Handler execution time in milliseconds")
                .build(),
            stop_total: meter
                .u64_counter("chroma.system.executor.stop_total")
                .with_description("Component executor stop outcomes")
                .build(),
            on_stop_latency_ms: meter
                .f64_histogram("chroma.system.executor.on_stop_latency_ms")
                .with_description("on_stop execution latency in milliseconds")
                .build(),
        }
    }
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
    ) -> (Self, Arc<ComponentRuntimeStats>) {
        let max_queue_depth = handler.queue_size();
        let runtime_stats = Arc::new(ComponentRuntimeStats::default());

        (
            ComponentExecutor {
                inner: Arc::new(Inner {
                    sender,
                    cancellation_token,
                    system,
                    scheduler,
                }),
                handler,
                metrics: ComponentExecutorMetrics::new(max_queue_depth),
                runtime_stats: runtime_stats.clone(),
            },
            runtime_stats,
        )
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
            self.metrics.queue_depth.record(
                queue_depth as u64,
                &[opentelemetry::KeyValue::new("component", C::get_name())],
            );

            let attrs = |result: &'static str| {
                [
                    opentelemetry::KeyValue::new("component", C::get_name()),
                    opentelemetry::KeyValue::new("result", result),
                ]
            };

            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    let stop_started = std::time::Instant::now();
                    match timeout(self.handler.on_stop_timeout(), self.handler.on_stop()).await {
                        Ok(Ok(())) => {
                            self.metrics.stop_total.add(1, &attrs("ok"));
                            self.metrics.on_stop_latency_ms.record(
                                duration_ms(stop_started.elapsed()),
                                &attrs("ok"),
                            );
                        }
                        Ok(Err(err)) => {
                            self.runtime_stats
                                .set_last_error(format!("on_stop error: {err}"));
                            self.metrics.stop_total.add(1, &attrs("error"));
                            self.metrics.on_stop_latency_ms.record(
                                duration_ms(stop_started.elapsed()),
                                &attrs("error"),
                            );
                            tracing::error!("Unable to gracefully shutdown {:?}: {err}", self.handler);
                        }
                        Err(err) => {
                            self.runtime_stats
                                .set_last_error(format!("on_stop timeout: {err}"));
                            self.metrics.stop_total.add(1, &attrs("timeout"));
                            self.metrics.on_stop_latency_ms.record(
                                duration_ms(stop_started.elapsed()),
                                &attrs("timeout"),
                            );
                            tracing::error!("Unable to gracefully shutdown {:?}: {err}", self.handler);
                        }
                    };
                    break;
                }
                message = channel.recv() => {
                    match message {
                        Some(mut message) => {
                            self.metrics.message_received_total.add(
                                1,
                                &[opentelemetry::KeyValue::new("component", C::get_name())],
                            );
                            let span: tracing::Span = message.get_tracing_context().unwrap_or(Span::current().clone());
                            let component_context = ComponentContext {
                                    system: self.inner.system.clone(),
                                    sender: self.inner.sender.clone(),
                                    cancellation_token: self.inner.cancellation_token.clone(),
                                    scheduler: self.inner.scheduler.clone(),
                            };
                            let started = std::time::Instant::now();
                            let task_future = message.handle(&mut self.handler, &component_context);
                            let outcome = task_future.instrument(span).await;
                            self.runtime_stats.record_message_handled();
                            match outcome {
                                crate::wrapped_message::MessageExecutionOutcome::Ok => {
                                    self.metrics.handler_completed_total.add(1, &attrs("ok"));
                                    self.metrics.handler_latency_ms.record(
                                        duration_ms(started.elapsed()),
                                        &attrs("ok"),
                                    );
                                }
                                crate::wrapped_message::MessageExecutionOutcome::Panic => {
                                    self.runtime_stats
                                        .set_last_error("handler panic".to_string());
                                    self.metrics.handler_completed_total.add(1, &attrs("panic"));
                                    self.metrics.handler_latency_ms.record(
                                        duration_ms(started.elapsed()),
                                        &attrs("panic"),
                                    );
                                }
                            }
                        }
                        None => {
                            self.runtime_stats
                                .set_last_error("component channel closed".to_string());
                            tracing::error!("Channel closed");
                        }
                    }
                }
            }
        }
    }
}
