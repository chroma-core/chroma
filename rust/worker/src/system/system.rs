use std::sync::Arc;

use futures::Stream;

use super::executor::StreamComponentExecutor;
use super::{executor, ComponentContext};
use super::{executor::ComponentExecutor, Component, ComponentHandle, Handler, StreamHandler};
use std::ptr;
use std::sync::Mutex;

#[derive(Clone)]
pub(crate) struct System {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    components: Vec<Arc<dyn Component + Send + Sync>>,
}

impl System {
    pub(crate) fn new() -> System {
        System {
            inner: Arc::new(Mutex::new(Inner {
                components: Vec::new(),
            })),
        }
    }

    pub(crate) fn start_component<C, M>(
        &mut self,
        component: C,
    ) -> (ComponentHandle, tokio::sync::broadcast::Sender<M>)
    where
        C: Handler<M> + Component + Send + Sync + 'static,
        M: Clone + Send + Sync + 'static,
    {
        let component = Arc::new(component);
        // Note: We lock inner since we only have minimal fields but
        // we can move to a more fine-grained locking scheme if needed.
        // System is not used in the critical path so this should be fine.
        match self.inner.lock() {
            Ok(mut inner) => {
                inner.components.push(component.clone());
            }
            Err(_) => {
                panic!("Failed to lock system");
            }
        }
        let (tx, rx) = tokio::sync::broadcast::channel(component.queue_size());
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let _ = component.on_start(&ComponentContext {
            system: self.clone(),
            sender: tx.clone(),
            cancellation_token: cancel_token.clone(),
            system_component: component.clone(),
        });
        let mut executor = ComponentExecutor::new(
            tx.clone(),
            cancel_token.clone(),
            component.clone(),
            component,
            self.clone(),
        );
        tokio::spawn(async move { executor.run(rx).await });
        return (ComponentHandle::new(cancel_token), tx);
    }

    pub(super) fn register_stream<C, M, S>(&self, stream: S, ctx: &ComponentContext<M, C>)
    where
        C: StreamHandler<M> + Component + Send + Sync + 'static,
        M: Clone + Send + Sync + 'static,
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        let mut executor = StreamComponentExecutor::new(
            ctx.sender.clone(),
            ctx.cancellation_token.clone(),
            ctx.system_component.clone(),
            ctx.system.clone(),
        );
        println!("Registering stream");
        tokio::spawn(async move { executor.run_from_stream(stream).await });
    }
}
