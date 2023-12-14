use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use tokio::select;

use super::{executor::ComponentExecutor, system::System};

#[derive(Debug, PartialEq)]
pub(crate) enum ComponentState {
    Running,
    Stopped,
}

pub(crate) trait Component {
    fn queue_size(&self) -> usize;
}

#[async_trait]
pub(crate) trait Handler<M>
where
    Self: Component + Sized + Send + Sync + 'static,
{
    async fn handle(&self, message: M, ctx: &ComponentContext<M, Self>) -> ();

    fn on_start(&self, ctx: &ComponentContext<M, Self>) -> () {}
}

#[async_trait]
pub(crate) trait StreamHandler<M>
where
    Self: Component + Sized + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    async fn handle(&self, message: M, ctx: &ComponentContext<M, Self>) -> ();

    fn register_stream<S>(&self, stream: S, ctx: &ComponentContext<M, Self>) -> ()
    where
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        ctx.system.register_stream(stream, ctx);
    }
}

pub(crate) struct ComponentHandle {
    cancellation_token: tokio_util::sync::CancellationToken,
    state: ComponentState,
}

impl ComponentHandle {
    pub(super) fn new(cancellation_token: tokio_util::sync::CancellationToken) -> Self {
        ComponentHandle {
            cancellation_token: cancellation_token,
            state: ComponentState::Running,
        }
    }

    pub(crate) fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.state = ComponentState::Stopped;
    }

    pub(crate) fn state(&self) -> &ComponentState {
        return &self.state;
    }
}

pub(crate) struct ComponentContext<M, C>
where
    C: Component + Send + Sync + 'static,
{
    pub(super) system: System,
    pub(super) sender: tokio::sync::broadcast::Sender<M>,
    pub(super) cancellation_token: tokio_util::sync::CancellationToken,
    pub(super) system_component: Arc<C>, // A reference to the component that is running in the system
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream; // Assuming you have the 'futures' crate

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
        async fn handle(
            &self,
            message: usize,
            _ctx: &ComponentContext<usize, TestComponent>,
        ) -> () {
            self.counter.fetch_add(message, Ordering::SeqCst);
        }

        fn on_start(&self, ctx: &ComponentContext<usize, TestComponent>) -> () {
            let test_stream = stream::iter(vec![1, 2, 3]);
            self.register_stream(test_stream, ctx);
        }
    }

    #[async_trait]
    impl StreamHandler<usize> for TestComponent {
        async fn handle(
            &self,
            message: usize,
            _ctx: &ComponentContext<usize, TestComponent>,
        ) -> () {
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
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        handle.stop();
        // Yield to allow the component to stop
        tokio::task::yield_now().await;
        assert_eq!(*handle.state(), ComponentState::Stopped);
        // With the streaming data and the messages we should have 12
        assert_eq!(counter.load(Ordering::SeqCst), 12);
        let res = tx.send(4);
        // Expect an error because the component is stopped
        assert!(res.is_err());
    }
}
