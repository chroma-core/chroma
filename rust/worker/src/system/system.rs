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
                let child_span =
                    trace_span!(parent: Span::current(), "component spawn", "name" = C::get_name());
                let task_future = async move { executor.run(rx).await };
                let join_handle = tokio::spawn(task_future.instrument(child_span));
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
                        let res = ctx.sender.send(message, None).await;
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
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct TestComponent {
        queue_size: usize,
        counter: Arc<AtomicUsize>,
    }

    impl TestComponent {
        fn new(queue_size: usize, counter: Arc<AtomicUsize>) -> Self {
            TestComponent {
                queue_size,
                counter,
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
            self.counter
                .fetch_add(message, std::sync::atomic::Ordering::SeqCst);
            return self.counter.load(std::sync::atomic::Ordering::SeqCst);
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

        async fn on_start(&mut self, ctx: &ComponentContext<TestComponent>) -> () {
            // let test_stream = stream::iter(vec![1, 2, 3]);
            // self.register_stream(test_stream, ctx);
        }
    }

    #[tokio::test]
    async fn response_types() {
        let system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let component = TestComponent::new(10, counter.clone());
        let mut handle = system.start_component(component);

        let result = handle.receiver().send(1, None).await.unwrap();
        println!("Result: {:?}", result);

        // let result = handle.receiver().send(1, None).await.unwrap();
        // handle.sender.send(1, None).await.unwrap();

        // // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // // With the streaming data and the messages we should have 12
        // assert_eq!(counter.load(Ordering::SeqCst), 12);
        // handle.stop();
        // // Yield to allow the component to stop
        // tokio::task::yield_now().await;
        // // Expect the component to be stopped
        // assert_eq!(*handle.state(), ComponentState::Stopped);
        // let res = handle.sender.send(4, None).await;
        // // Expect an error because the component is stopped
        // assert!(res.is_err());
    }
}
