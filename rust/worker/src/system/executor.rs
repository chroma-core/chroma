use futures::StreamExt;
use std::sync::Arc;

use futures::Stream;
use tokio::{pin, select};

use crate::system::ComponentContext;

use super::{system::System, Component, Handler, StreamHandler};

struct Inner<C, M>
where
    C: Component + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    pub(super) channel_in: tokio::sync::broadcast::Sender<M>,
    pub(super) cancellation_token: tokio_util::sync::CancellationToken,
    pub(super) system_component: Arc<C>,
    pub(super) system: System,
}

#[derive(Clone)]
pub(super) struct ComponentExecutor<H, M>
where
    H: Handler<M> + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    inner: Arc<Inner<H, M>>,
    handler: Arc<H>,
}

impl<H, M> ComponentExecutor<H, M>
where
    H: Handler<M> + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        channel_in: tokio::sync::broadcast::Sender<M>,
        cancellation_token: tokio_util::sync::CancellationToken,
        system_component: Arc<H>,
        handler: Arc<H>,
        system: System,
    ) -> Self {
        ComponentExecutor {
            inner: Arc::new(Inner {
                channel_in,
                cancellation_token,
                system_component,
                system,
            }),
            handler,
        }
    }

    pub(super) async fn run(&mut self, mut channel: tokio::sync::broadcast::Receiver<M>) {
        loop {
            select! {
                    _ = self.inner.cancellation_token.cancelled() => {
                        println!("RUN Cancellation token cancelled");
                        break;
                    }
                    message = channel.recv() => {
                        match message {
                            Ok(message) => {
                                self.handler.handle(message,
                                    &ComponentContext{
                                        system: self.inner.system.clone(),
                                        sender: self.inner.channel_in.clone(),
                                        cancellation_token: self.inner.cancellation_token.clone(),
                                        system_component: self.inner.system_component.clone(),
                                    }
                                ).await;
                            }
                            Err(_) => {
                                // TODO: Log error
                            }
                        }
                }
            }
        }
    }
}

#[derive(Clone)]
pub(super) struct StreamComponentExecutor<H, M>
where
    H: StreamHandler<M> + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    inner: Arc<Inner<H, M>>,
    handler: Arc<H>,
}

impl<H, M> StreamComponentExecutor<H, M>
where
    H: StreamHandler<M> + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        channel_in: tokio::sync::broadcast::Sender<M>,
        cancellation_token: tokio_util::sync::CancellationToken,
        handler: Arc<H>,
        system: System,
    ) -> Self {
        StreamComponentExecutor {
            inner: Arc::new(Inner {
                channel_in,
                cancellation_token,
                system_component: handler.clone(),
                system,
            }),
            handler,
        }
    }

    pub(super) async fn run_from_stream<S>(&mut self, stream: S)
    where
        S: Stream<Item = M>,
    {
        println!("Running from stream");
        pin!(stream);
        loop {
            select! {
                _ = self.inner.cancellation_token.cancelled() => {
                    println!("STREAM Cancellation token cancelled");
                    break;
                }
                message = stream.next() => {
                    match message {
                        Some(message) => {
                            println!("HI Message");
                            self.handler.handle(message, &ComponentContext{system: self.inner.system.clone(), sender: self.inner.channel_in.clone(), cancellation_token: self.inner.cancellation_token.clone(), system_component: self.inner.system_component.clone()}).await;
                        }
                        None => {
                            println!("No message");
                            break;
                        }
                    }
                }
            }
        }
    }
}
