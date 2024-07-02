use super::{
    scheduler::Scheduler, ChannelError, MessageHandlerError, RequestError, WrappedMessage,
};
use async_trait::async_trait;
use core::panic;
use futures::Stream;
use parking_lot::Mutex;
use std::{fmt::Debug, sync::Arc};
use tokio::task::JoinError;

use super::{system::System, ReceiverForMessage};

pub(super) trait Message: Debug + Send + 'static {}
impl<M: Debug + Send + 'static> Message for M {}

#[derive(Debug, PartialEq, Clone, Copy)]
/// The state of a component
/// A component can be running or stopped
/// A component is stopped when it is cancelled
/// A component can be run with a system
pub(crate) enum ComponentState {
    Running,
    Stopped,
}

#[derive(Debug, PartialEq, Clone, Copy)]
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
    fn get_name() -> &'static str;
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
    type Result: Send + Debug + 'static;

    async fn handle(&mut self, message: M, ctx: &ComponentContext<Self>) -> Self::Result
    // The need for this lifetime bound comes from the async_trait macro when we need generic lifetimes in our message type
    // https://stackoverflow.com/questions/69560112/how-to-use-rust-async-trait-generic-to-a-lifetime-parameter
    where
        M: 'async_trait;
}

/// A stream handler is a component that can process messages of a given type from a stream.
/// # Methods
/// - handle: Handle a message from a stream
/// - register_stream: Register a stream to be processed, this is provided and you do not need to implement it
pub(crate) trait StreamHandler<M>
where
    Self: Component + 'static + Handler<M>,
    M: Message,
{
    fn register_stream<S>(&self, stream: S, ctx: &ComponentContext<Self>) -> ()
    where
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        ctx.system.register_stream(stream, ctx);
    }
}

/// A thin wrapper over a join handle that will panic if it is consumed more than once.
#[derive(Debug, Clone)]
struct ConsumableJoinHandle {
    handle: Option<Arc<tokio::task::JoinHandle<()>>>,
}

impl ConsumableJoinHandle {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        ConsumableJoinHandle {
            handle: Some(Arc::new(handle)),
        }
    }

    async fn consume(&mut self) -> Result<(), JoinError> {
        match self.handle.take() {
            Some(handle) => {
                let handle = Arc::into_inner(handle)
                    .expect("there should be no other strong references to the join handle");
                handle.await?;
                Ok(())
            }
            None => {
                panic!("Join handle already consumed");
            }
        }
    }
}

/// A ComponentSender is generic over a component type. This struct is internal to the system module.
/// It's implemented as a struct instead of a type alias so that it can implement common logic around the channel.
///
/// See ReceiverForMessage for a trait generic over a message type.
#[derive(Debug)]
pub(super) struct ComponentSender<C: Component> {
    sender: tokio::sync::mpsc::Sender<WrappedMessage<C>>,
}

impl<C: Component> ComponentSender<C> {
    pub(super) fn new(sender: tokio::sync::mpsc::Sender<WrappedMessage<C>>) -> Self {
        ComponentSender { sender }
    }

    pub(super) async fn wrap_and_send<M>(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>
    where
        C: Handler<M>,
        M: Message,
    {
        self.sender
            .send(WrappedMessage::new(message, None, tracing_context))
            .await
            .map_err(|_| ChannelError::SendError)
    }

    pub(super) async fn wrap_and_request<M>(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<C::Result, RequestError>
    where
        C: Handler<M>,
        M: Message,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WrappedMessage::new(message, Some(tx), tracing_context))
            .await
            .map_err(|_| RequestError::SendError)?;

        let result = rx.await.map_err(|_| RequestError::ReceiveError)?;

        match result {
            Ok(result) => Ok(result),
            Err(err) => match err {
                MessageHandlerError::Panic(p) => Err(RequestError::HandlerPanic(p)),
            },
        }
    }
}

// Cannot automatically derive, see https://github.com/rust-lang/rust/issues/26925
impl<C: Component> Clone for ComponentSender<C> {
    fn clone(&self) -> Self {
        ComponentSender {
            sender: self.sender.clone(),
        }
    }
}

/// A component handle is a handle to a component that can be used to stop it.
/// and introspect its state.
/// # Fields
/// - cancellation_token: A cancellation token that can be used to stop the component
/// - state: The state of the component
/// - join_handle: The join handle for the component, used to join on the component
/// - sender: A channel to send messages to the component
#[derive(Debug)]
pub(crate) struct ComponentHandle<C: Component + Debug> {
    cancellation_token: tokio_util::sync::CancellationToken,
    state: Arc<Mutex<ComponentState>>,
    join_handle: Option<ConsumableJoinHandle>,
    sender: ComponentSender<C>,
}

// Implemented manually because of https://github.com/rust-lang/rust/issues/26925.
impl<C: Component> Clone for ComponentHandle<C> {
    fn clone(&self) -> Self {
        ComponentHandle {
            cancellation_token: self.cancellation_token.clone(),
            state: self.state.clone(),
            join_handle: self.join_handle.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<C: Component> ComponentHandle<C> {
    pub(super) fn new(
        cancellation_token: tokio_util::sync::CancellationToken,
        // Components with a dedicated runtime do not have a join handle
        // and instead use a one shot channel to signal completion
        // TODO: implement this
        join_handle: Option<tokio::task::JoinHandle<()>>,
        sender: ComponentSender<C>,
    ) -> Self {
        ComponentHandle {
            cancellation_token: cancellation_token,
            state: Arc::new(Mutex::new(ComponentState::Running)),
            join_handle: join_handle.map(|handle| ConsumableJoinHandle::new(handle)),
            sender: sender,
        }
    }

    pub(crate) fn stop(&mut self) {
        let mut state = self.state.lock();
        self.cancellation_token.cancel();
        *state = ComponentState::Stopped;
    }

    /// Consumes the underlying join handle. Panics if it is consumed twice.
    pub(crate) async fn join(&mut self) -> Result<(), JoinError> {
        if let Some(join_handle) = &mut self.join_handle {
            join_handle.consume().await
        } else {
            Ok(())
        }
    }

    pub(crate) async fn state(&self) -> ComponentState {
        return *self.state.lock();
    }

    pub(crate) fn receiver<M>(&self) -> Box<dyn ReceiverForMessage<M>>
    where
        C: Component + Handler<M>,
        M: Message,
    {
        Box::new(self.sender.clone())
    }

    pub(crate) async fn send<M>(
        &mut self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>
    where
        C: Handler<M>,
        M: Message,
    {
        self.sender.wrap_and_send(message, tracing_context).await
    }

    pub(crate) async fn request<M>(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<C::Result, RequestError>
    where
        C: Handler<M>,
        M: Message,
    {
        self.sender.wrap_and_request(message, tracing_context).await
    }
}

/// The component context is passed to all Component Handler methods
pub(crate) struct ComponentContext<C>
where
    C: Component + 'static,
{
    pub(crate) system: System,
    pub(crate) sender: ComponentSender<C>,
    pub(crate) cancellation_token: tokio_util::sync::CancellationToken,
    pub(crate) scheduler: Scheduler,
}

impl<C: Component> ComponentContext<C> {
    pub(crate) fn receiver<M>(&self) -> Box<dyn ReceiverForMessage<M>>
    where
        C: Component + Handler<M>,
        M: Message,
    {
        Box::new(self.sender.clone())
    }

    pub(crate) async fn send<M>(
        &self,
        message: M,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), ChannelError>
    where
        C: Handler<M>,
        M: Message,
    {
        self.sender.wrap_and_send(message, tracing_context).await
    }
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
        type Result = ();

        async fn handle(&mut self, message: usize, _ctx: &ComponentContext<TestComponent>) -> () {
            self.counter.fetch_add(message, Ordering::SeqCst);
        }
    }
    impl StreamHandler<usize> for TestComponent {}

    #[async_trait]
    impl Component for TestComponent {
        fn get_name() -> &'static str {
            "Test component"
        }

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
        handle.send(1, None).await.unwrap();
        handle.send(2, None).await.unwrap();
        handle.send(3, None).await.unwrap();
        // yield to allow the component to process the messages
        tokio::task::yield_now().await;
        // With the streaming data and the messages we should have 12
        assert_eq!(counter.load(Ordering::SeqCst), 12);
        handle.stop();
        // Yield to allow the component to stop
        tokio::task::yield_now().await;
        // Expect the component to be stopped
        assert_eq!(handle.state().await, ComponentState::Stopped);
        let res = handle.send(4, None).await;
        // Expect an error because the component is stopped
        assert!(res.is_err());
    }

    #[tokio::test]
    #[should_panic(expected = "Join handle already consumed")]
    async fn join_handle_panics_if_consumed_twice() {
        let handle = tokio::spawn(async {});
        let mut handle = ConsumableJoinHandle::new(handle);

        handle.consume().await.unwrap();
        // Expected to panic
        handle.consume().await.unwrap();
    }
}
