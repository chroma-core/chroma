use super::scheduler::Scheduler;
use super::ComponentContext;
use super::ComponentRuntime;
use super::ComponentSender;
use super::ConsumableJoinHandle;
use super::Message;
use super::{executor::ComponentExecutor, Component, ComponentHandle, Handler, StreamHandler};
use futures::Stream;
use futures::StreamExt;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::{pin, select};
use tracing::{trace_span, Instrument, Span};

#[derive(Clone, Debug)]
pub(crate) struct System {
    inner: Arc<Inner>,
}

#[derive(Debug)]
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

    pub(crate) fn start_component<C>(&self, component: C) -> ComponentHandle<C>
    where
        C: Component + Send + 'static,
    {
        let (tx, rx) = tokio::sync::mpsc::channel(component.queue_size());
        let sender: ComponentSender<C> = ComponentSender::new(tx);
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
                let child_span =
                    trace_span!(parent: Span::current(), "component spawn", "name" = C::get_name());
                let task_future = async move { executor.run(rx).await };
                let join_handle = tokio::spawn(task_future.instrument(child_span));
                return ComponentHandle::new(
                    cancel_token,
                    Some(ConsumableJoinHandle::new(join_handle)),
                    sender,
                );
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
        M: Message,
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
    M: Message,
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
                        let res = ctx.send(message, None).await;
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

#[cfg(test)]
mod tests {
    use crate::system::RequestError;

    use super::*;
    use async_trait::async_trait;

    #[derive(Debug)]
    struct TestComponent {
        queue_size: usize,
        counter: usize,
    }

    impl TestComponent {
        fn new(queue_size: usize) -> Self {
            TestComponent {
                queue_size,
                counter: 0,
            }
        }
    }

    #[async_trait]
    impl Handler<usize> for TestComponent {
        type Result = usize;

        async fn handle(
            &mut self,
            message: usize,
            _ctx: &ComponentContext<TestComponent>,
        ) -> Self::Result {
            if message == 0 {
                panic!("Invalid input");
            }

            self.counter += message;
            return self.counter;
        }
    }

    #[async_trait]
    impl Component for TestComponent {
        fn get_name() -> &'static str {
            "Test component"
        }

        fn queue_size(&self) -> usize {
            self.queue_size
        }
    }

    #[tokio::test]
    async fn response_types() {
        let system = System::new();
        let component = TestComponent::new(10);
        let handle = system.start_component(component);

        assert_eq!(1, handle.request(1, None).await.unwrap());
        assert_eq!(2, handle.request(1, None).await.unwrap());
    }

    #[tokio::test]
    async fn catches_panic() {
        let system = System::new();
        let component = TestComponent::new(10);
        let handle = system.start_component(component);

        let err = handle.request(0, None).await.unwrap_err();
        assert_eq!(
            RequestError::HandlerPanic(Some("Invalid input".to_string())),
            err
        );

        // Component is still alive
        assert_eq!(1, handle.request(1, None).await.unwrap());
    }
}
