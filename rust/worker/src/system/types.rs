use super::scheduler::Scheduler;
use async_trait::async_trait;
use futures::Stream;
use std::fmt::Debug;

use super::{sender::Sender, system::System, Receiver, ReceiverImpl};

#[derive(Debug, PartialEq)]
/// The state of a component
/// A component can be running or stopped
/// A component is stopped when it is cancelled
/// A component can be run with a system
pub(crate) enum ComponentState {
    Running,
    Stopped,
}

#[derive(Debug, PartialEq)]
pub(crate) enum ComponentRuntime {
    Inherit,
    Dedicated,
}

/// A component is a processor of work that can be run in a system.
/// It has a queue of messages that it can process.
/// Others can send messages to the component.
/// A component can be stopped using its handle.
/// It is a data object, and stores some parameterization
/// for how the system should run it.
/// # Methods
/// - queue_size: The size of the queue to use for the component before it starts dropping messages
/// - on_start: Called when the component is started
#[async_trait]
pub(crate) trait Component: Send + Sized + Debug + 'static {
    fn queue_size(&self) -> usize;
    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Inherit
    }
    async fn on_start(&mut self, _ctx: &ComponentContext<Self>) -> () {}
}

/// A handler is a component that can process messages of a given type.
/// # Methods
/// - handle: Handle a message
#[async_trait]
pub(crate) trait Handler<M>
where
    Self: Component + Sized + 'static,
{
    async fn handle(&mut self, message: M, ctx: &ComponentContext<Self>) -> ();
}

/// A stream handler is a component that can process messages of a given type from a stream.
/// # Methods
/// - handle: Handle a message from a stream
/// - register_stream: Register a stream to be processed, this is provided and you do not need to implement it
pub(crate) trait StreamHandler<M>
where
    Self: Component + 'static + Handler<M>,
    M: Send + Debug + 'static,
{
    fn register_stream<S>(&self, stream: S, ctx: &ComponentContext<Self>) -> ()
    where
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        ctx.system.register_stream(stream, ctx);
    }
}

/// A component handle is a handle to a component that can be used to stop it.
/// and introspect its state.
/// # Fields
/// - cancellation_token: A cancellation token that can be used to stop the component
/// - state: The state of the component
/// - join_handle: The join handle for the component, used to join on the component
pub(crate) struct ComponentHandle<C: Component> {
    cancellation_token: tokio_util::sync::CancellationToken,
    state: ComponentState,
    join_handle: Option<tokio::task::JoinHandle<()>>,
    sender: Sender<C>,
}

impl<C: Component> ComponentHandle<C> {
    pub(super) fn new(
        cancellation_token: tokio_util::sync::CancellationToken,
        // Components with a dedicated runtime do not have a join handle
        // and instead use a one shot channel to signal completion
        // TODO: implement this
        join_handle: Option<tokio::task::JoinHandle<()>>,
        sender: Sender<C>,
    ) -> Self {
        ComponentHandle {
            cancellation_token: cancellation_token,
            state: ComponentState::Running,
            join_handle: join_handle,
            sender: sender,
        }
    }

    pub(crate) fn stop(&mut self) {
        self.cancellation_token.cancel();
        self.state = ComponentState::Stopped;
    }

    pub(crate) async fn join(&mut self) {
        match self.join_handle.take() {
            Some(handle) => {
                handle.await;
            }
            None => return,
        };
    }

    pub(crate) fn state(&self) -> &ComponentState {
        return &self.state;
    }

    pub(crate) fn receiver<M>(&self) -> Box<dyn Receiver<M> + Send>
    where
        C: Handler<M>,
        M: Send + Debug + 'static,
    {
        let sender = self.sender.sender.clone();
        Box::new(ReceiverImpl::new(sender))
    }
}

/// The component context is passed to all Component Handler methods
pub(crate) struct ComponentContext<C>
where
    C: Component + 'static,
{
    pub(crate) system: System,
    pub(crate) sender: Sender<C>,
    pub(crate) cancellation_token: tokio_util::sync::CancellationToken,
    pub(crate) scheduler: Scheduler,
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use futures::stream;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

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
        async fn handle(&mut self, message: usize, _ctx: &ComponentContext<TestComponent>) -> () {
            self.counter.fetch_add(message, Ordering::SeqCst);
        }
    }
    impl StreamHandler<usize> for TestComponent {}

    #[async_trait]
    impl Component for TestComponent {
        fn queue_size(&self) -> usize {
            self.queue_size
        }

        async fn on_start(&mut self, ctx: &ComponentContext<TestComponent>) -> () {
            let test_stream = stream::iter(vec![1, 2, 3]);
            self.register_stream(test_stream, ctx);
        }
    }

    #[tokio::test]
    async fn it_can_work() {
        let system = System::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let component = TestComponent::new(10, counter.clone());
        let mut handle = system.start_component(component);
        handle.sender.send(1, None).await.unwrap();
        handle.sender.send(2, None).await.unwrap();
        handle.sender.send(3, None).await.unwrap();
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // With the streaming data and the messages we should have 12
        assert_eq!(counter.load(Ordering::SeqCst), 12);
        handle.stop();
        // Yield to allow the component to stop
        tokio::task::yield_now().await;
        // Expect the component to be stopped
        assert_eq!(*handle.state(), ComponentState::Stopped);
        let res = handle.sender.send(4, None).await;
        // Expect an error because the component is stopped
        assert!(res.is_err());
    }
}
