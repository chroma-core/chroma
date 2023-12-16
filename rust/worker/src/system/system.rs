use std::fmt::Debug;
use std::sync::Arc;

use futures::Stream;
use futures::StreamExt;
use tokio::{pin, select};

// use super::executor::StreamComponentExecutor;
use super::sender::{self, Sender, Wrapper};
use super::{executor, ComponentContext};
use super::{executor::ComponentExecutor, Component, ComponentHandle, Handler, StreamHandler};
use std::sync::Mutex;

#[derive(Clone)]
pub(crate) struct System {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {}

impl System {
    pub(crate) fn new() -> System {
        System {
            inner: Arc::new(Mutex::new(Inner {})),
        }
    }

    pub(crate) fn start_component<C>(&mut self, component: C) -> ComponentHandle<C>
    where
        C: Component + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender = Sender::new(tx);
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let _ = component.on_start(&ComponentContext {
            system: self.clone(),
            sender: sender.clone(),
            cancellation_token: cancel_token.clone(),
        });
        let mut executor = ComponentExecutor::new(
            sender.clone(),
            cancel_token.clone(),
            component,
            self.clone(),
        );
        let join_handle = tokio::spawn(async move { executor.run(rx).await });
        return ComponentHandle::new(cancel_token, join_handle, sender);
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
        };
        tokio::spawn(async move { stream_loop(stream, &ctx).await });
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
