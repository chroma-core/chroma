use super::{scheduler::Scheduler, ChannelError, RequestError, WrappedMessage};
use async_trait::async_trait;
use chroma_config::registry::Injectable;
use chroma_error::ChromaError;
use core::panic;
use futures::Stream;
use parking_lot::Mutex;
use std::{fmt::Debug, sync::Arc, time::Duration};
use thiserror::Error;

use super::{system::System, ReceiverForMessage};

pub trait Message: Debug + Send + 'static {}
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
pub enum ComponentRuntime {
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
pub trait Component: Send + Sized + Debug + 'static {
    fn get_name() -> &'static str;
    fn queue_size(&self) -> usize;
    fn runtime() -> ComponentRuntime {
        ComponentRuntime::Inherit
    }
    async fn on_start(&mut self, _ctx: &ComponentContext<Self>) -> () {}
    async fn on_stop(&mut self) -> Result<(), Box<dyn ChromaError>> {
        Ok(())
    }
    fn on_stop_timeout(&self) -> Duration {
        Duration::from_secs(6)
    }
    fn on_handler_panic(&mut self, panic: Box<dyn core::any::Any + Send>) {
        // Default behavior is to log and then resume the panic
        tracing::error!("Handler panicked: {:?}", panic);
        std::panic::resume_unwind(panic);
    }
}

/// A handler is a component that can process messages of a given type.
/// # Methods
/// - handle: Handle a message
#[async_trait]
pub trait Handler<M>
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
pub trait StreamHandler<M>
where
    Self: Component + 'static + Handler<M>,
    M: Message,
{
    fn register_stream<S>(&self, stream: S, ctx: &ComponentContext<Self>)
    where
        S: Stream + Send + Stream<Item = M> + 'static,
    {
        ctx.system.register_stream(stream, ctx);
    }
}

/// A thin wrapper over a join handle that will panic if it is consumed more than once.
#[derive(Debug, Clone)]
pub(super) enum ConsumableJoinHandle {
    TokioTask(Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>),
    Thread(Arc<Mutex<Option<std::thread::JoinHandle<()>>>>),
}

#[derive(Debug, Error)]
pub enum ConsumeJoinHandleError {
    #[error("Tokio task failed: {0}")]
    TokioTaskFailed(#[from] tokio::task::JoinError),
    #[error("Thread panicked")]
    ThreadPanicked(Box<dyn std::any::Any + Send + 'static>),
}

impl ConsumableJoinHandle {
    pub(super) fn from_tokio_task_handle(handle: tokio::task::JoinHandle<()>) -> Self {
        ConsumableJoinHandle::TokioTask(Arc::new(Mutex::new(Some(handle))))
    }

    pub(super) fn from_thread_handle(handle: std::thread::JoinHandle<()>) -> Self {
        ConsumableJoinHandle::Thread(Arc::new(Mutex::new(Some(handle))))
    }

    async fn consume(&mut self) -> Result<(), ConsumeJoinHandleError> {
        match self {
            ConsumableJoinHandle::TokioTask(handle) => {
                let handle = { handle.lock().take() };
                match handle {
                    Some(handle) => {
                        handle.await?;
                        Ok(())
                    }
                    None => {
                        panic!("Join handle already consumed");
                    }
                }
            }
            ConsumableJoinHandle::Thread(handle) => {
                let handle = { handle.lock().take() };
                match handle {
                    Some(handle) => {
                        tokio::task::spawn_blocking(move || {
                            handle
                                .join()
                                .map_err(ConsumeJoinHandleError::ThreadPanicked)?;
                            Ok(())
                        })
                        .await?
                    }
                    None => {
                        panic!("Join handle already consumed");
                    }
                }
            }
        }
    }
}

/// A ComponentSender is generic over a component type. This struct is internal to the system module.
/// It's implemented as a struct instead of a type alias so that it can implement common logic around the channel.
///
/// See ReceiverForMessage for a trait generic over a message type.
#[derive(Debug)]
pub(crate) struct ComponentSender<C: Component> {
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
            .try_send(WrappedMessage::new(message, None, tracing_context))
            .map_err(|error| ChannelError::SendError(error.to_string()))
    }

    #[allow(dead_code)]
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

        Ok(result)
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
pub struct ComponentHandle<C: Component + Debug> {
    cancellation_token: tokio_util::sync::CancellationToken,
    state: Arc<Mutex<ComponentState>>,
    join_handle: Option<ConsumableJoinHandle>,
    sender: ComponentSender<C>,
}

// Blanket implementation for all components of the Injectable trait
impl<C> Injectable for ComponentHandle<C> where C: Component {}

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
        join_handle: Option<ConsumableJoinHandle>,
        sender: ComponentSender<C>,
    ) -> Self {
        ComponentHandle {
            cancellation_token,
            state: Arc::new(Mutex::new(ComponentState::Running)),
            join_handle,
            sender,
        }
    }

    pub fn stop(&mut self) {
        let mut state = self.state.lock();
        self.cancellation_token.cancel();
        *state = ComponentState::Stopped;
    }

    /// Consumes the underlying join handle. Panics if it is consumed twice.
    pub async fn join(&mut self) -> Result<(), ConsumeJoinHandleError> {
        if let Some(join_handle) = &mut self.join_handle {
            join_handle.consume().await
        } else {
            Ok(())
        }
    }

    #[cfg(test)]
    pub(crate) async fn state(&self) -> ComponentState {
        return *self.state.lock();
    }

    pub fn receiver<M>(&self) -> Box<dyn ReceiverForMessage<M>>
    where
        C: Component + Handler<M>,
        M: Message,
    {
        Box::new(self.sender.clone())
    }

    pub async fn send<M>(
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

    pub async fn request<M>(
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
pub struct ComponentContext<C>
where
    C: Component + 'static,
{
    pub(crate) system: System,
    pub(crate) sender: ComponentSender<C>,
    pub cancellation_token: tokio_util::sync::CancellationToken,
    pub scheduler: Scheduler,
}

impl<C: Component> ComponentContext<C> {
    pub fn receiver<M>(&self) -> Box<dyn ReceiverForMessage<M>>
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
        let mut handle = ConsumableJoinHandle::from_tokio_task_handle(handle);
        // Should be able to clone the handle
        let mut cloned = handle.clone();

        cloned.consume().await.unwrap();
        // Expected to panic
        handle.consume().await.unwrap();
    }
}
