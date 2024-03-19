use super::scheduler::Scheduler;
use super::sender::Sender;
use super::ComponentContext;
use super::ComponentRuntime;
use super::{executor::ComponentExecutor, Component, ComponentHandle, Handler, StreamHandler};
use futures::Stream;
use futures::StreamExt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::{pin, select};

#[derive(Clone)]
pub(crate) struct System {
    inner: Arc<Inner>,
}

struct Inner {
    scheduler: Scheduler,
}

impl System {
    pub(crate) fn new() -> System {
        System {
            inner: Arc::new(Inner {
                scheduler: Scheduler::new(),
            }),
        }
    }

    pub(crate) fn start_component<C>(&mut self, component: C) -> ComponentHandle<C>
    where
        C: Component + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender = Sender::new(tx);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let mut executor = ComponentExecutor::new(
            sender.clone(),
            cancel_token.clone(),
            component,
            self.clone(),
            self.inner.scheduler.clone(),
        );

        match C::runtime() {
            ComponentRuntime::Inherit => {
                let join_handle = tokio::spawn(async move { executor.run(rx).await });
                return ComponentHandle::new(cancel_token, Some(join_handle), sender);
            }
            ComponentRuntime::Dedicated => {
                println!("Spawning on dedicated thread");
                // Spawn on a dedicated thread
                let rt = Builder::new_current_thread().enable_all().build().unwrap();
                let join_handle = std::thread::spawn(move || {
                    rt.block_on(async move { executor.run(rx).await });
                });
                // TODO: Implement Join for dedicated threads
                return ComponentHandle::new(cancel_token, None, sender);
            }
        }
    }

    pub(super) fn register_stream<C, S, M>(&self, stream: S, ctx: &ComponentContext<C>)
    where
        C: StreamHandler<M> + Handler<M>,
        M: Send + Debug + 'static,
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        let ctx = ComponentContext {
            system: self.clone(),
            sender: ctx.sender.clone(),
            cancellation_token: ctx.cancellation_token.clone(),
            scheduler: ctx.scheduler.clone(),
        };
        tokio::spawn(async move { stream_loop(stream, &ctx).await });
    }

    pub(crate) async fn stop(&self) {
        self.inner.scheduler.stop();
    }

    pub(crate) async fn join(&self) {
        self.inner.scheduler.join().await;
    }
}

async fn stream_loop<C, S, M>(stream: S, ctx: &ComponentContext<C>)
where
    C: StreamHandler<M> + Handler<M>,
    M: Send + Debug + 'static,
    S: Stream + Send + Stream<Item = M> + 'static,
{
    pin!(stream);
    loop {
        select! {
            _ = ctx.cancellation_token.cancelled() => {
                break;
            }
            message = stream.next() => {
                match message {
                    Some(message) => {
                        let res = ctx.sender.send(message).await;
                        match res {
                            Ok(_) => {}
                            Err(e) => {
                                println!("Failed to send message: {:?}", e);
                                // TODO: switch to logging
                                // Terminate the stream
                                break;
                            }
                        }
                    },
                    None => {
                        break;
                    }
                }
            }
        }
    }
}
