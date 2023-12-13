use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use tokio::select;

#[derive(Debug, PartialEq)]
pub(crate) enum ComponentState {
    Running,
    Stopped,
}

pub(crate) trait Component {
    fn queue_size(&self) -> usize;
}

#[async_trait]
pub(crate) trait Handler<M> {
    async fn handle(&self, message: M);
}

pub(crate) struct ComponentHandle {
    cancellation_token: tokio_util::sync::CancellationToken,
    state: ComponentState,
}

impl ComponentHandle {
    fn new(cancellation_token: tokio_util::sync::CancellationToken) -> Self {
        ComponentHandle {
            cancellation_token: cancellation_token,
            state: ComponentState::Running,
        }
    }

    fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.state = ComponentState::Stopped;
    }

    fn state(&self) -> &ComponentState {
        return &self.state;
    }
}
struct ComponentExecutor<M> {
    channel: tokio::sync::broadcast::Receiver<M>,
    cancellation_token: tokio_util::sync::CancellationToken,
    handler: Arc<dyn Handler<M> + Send + Sync>,
}

impl<M: Clone> ComponentExecutor<M> {
    async fn run(&mut self) {
        loop {
            select! {
                    _ = self.cancellation_token.cancelled() => {
                        break;
                    }
                    message = self.channel.recv() => {
                        match message {
                            Ok(message) => {
                                self.handler.handle(message).await;
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

pub(crate) struct System {
    components: Vec<Arc<dyn Component + Send + Sync>>,
}

impl System {
    pub(crate) fn new() -> System {
        System {
            components: Vec::new(),
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
        self.components.push(component.clone());
        let (tx, rx) = tokio::sync::broadcast::channel(component.queue_size());
        let cancel_token = tokio_util::sync::CancellationToken::new();
        let mut executor = ComponentExecutor {
            channel: rx,
            handler: component,
            cancellation_token: cancel_token.clone(),
        };
        tokio::spawn(async move { executor.run().await });
        return (ComponentHandle::new(cancel_token), tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestComponent {
        queue_size: usize,
        counter: Arc<AtomicUsize>,
    }

    impl TestComponent {
        fn new(queue_size: usize, counter: Arc<AtomicUsize>) -> Self {
            TestComponent {
                queue_size: queue_size,
                counter: counter,
            }
        }
    }

    #[async_trait]
    impl Handler<usize> for TestComponent {
        async fn handle(&self, message: usize) {
            self.counter.fetch_add(message, Ordering::SeqCst);
        }
    }

    impl Component for TestComponent {
        fn queue_size(&self) -> usize {
            return self.queue_size;
        }
    }

    #[tokio::test]
    async fn it_can_work() {
        let mut system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let component = TestComponent::new(10, counter.clone());
        let (mut handle, tx) = system.start_component(component);
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        tx.send(3).unwrap();
        // Sleep for a bit to allow the component to process the messages
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        handle.stop();
        // Yield to allow the component to stop
        tokio::task::yield_now().await;
        assert_eq!(*handle.state(), ComponentState::Stopped);
        assert_eq!(counter.load(Ordering::SeqCst), 6);
        let res = tx.send(4);
        // Expect an error because the component is stopped
        assert!(res.is_err());
    }
}
