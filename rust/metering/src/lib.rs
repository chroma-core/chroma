extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;

use crate::{
    attributes::generate_attribute_definition_token_stream,
    events::generate_event_definition_token_stream,
    mutators::generate_noop_mutator_definition_token_stream,
    utils::{generate_compile_error, process_token_stream},
};

mod annotations;
mod attributes;
mod errors;
mod events;
mod fields;
mod mutators;
mod utils;

/// This is the only user-facing export of `chroma_metering`. It is responsible for registering attributes and
/// events by producing the code necessary to allow users to interact with the metering library.
#[proc_macro]
pub fn initialize_metering(raw_token_stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let token_stream = TokenStream::from(raw_token_stream);

    let (attributes, events) = match process_token_stream(&token_stream) {
        Ok(result) => result,
        Err(error) => return generate_compile_error(&error.to_string()),
    };

    let noop_mutator_definition_token_streams: Vec<TokenStream> = attributes
        .iter()
        .map(generate_noop_mutator_definition_token_stream)
        .collect();

    let attribute_definition_token_streams: Vec<TokenStream> = attributes
        .iter()
        .map(generate_attribute_definition_token_stream)
        .collect();

    let event_definition_token_streams: Vec<TokenStream> = events
        .iter()
        .map(generate_event_definition_token_stream)
        .collect();

    proc_macro::TokenStream::from(quote! {
        /// The primary trait used in the metering library that contains no-op mutators for every attribute.
        pub trait MeteringEvent: std::fmt::Debug + std::any::Any + Send + 'static {
            #( #noop_mutator_definition_token_streams )*
        }

        #( #attribute_definition_token_streams )*

        #( #event_definition_token_streams )*

        /// The default receiver registered in the library.
        #[derive(Clone, std::fmt::Debug)]

        pub struct __DefaultReceiver;

        /// The default receiver simply prints out the metering events submitted to it.
        #[async_trait::async_trait]
        impl chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>
            for __DefaultReceiver
        {
            async fn send(
                &self,
                message: Box<dyn MeteringEvent>,
                tracing_context: Option<tracing::Span>,
            ) -> Result<(), chroma_system::ChannelError> {
                if let Some(span) = tracing_context {
                    println!("[metering] span={:?} event={:?}", span, message);
                } else {
                    println!("[metering] event={:?}", message);
                }
                Ok(())
            }
        }

        /// The storage slot for the registered receiver.
        static RECEIVER: once_cell::sync::Lazy<
            parking_lot::Mutex<Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>>>,
        > = once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(Box::new(__DefaultReceiver)));

        /// Allows library users to register their own receivers.
        pub fn register_receiver(
            receiver: Box<
                dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
            >,
        ) {
            let mut receiver_slot = RECEIVER.lock();
            *receiver_slot = receiver;
        }

        /// A trait containing a `submit` method to send metering events to the registered receiver.
        #[async_trait::async_trait]
        pub trait SubmitExt: MeteringEvent + Sized + Send {
            async fn submit(self) {
                let maybe_current_span = Some(tracing::Span::current());

                let receiver: Box<
                    dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
                > = {
                    let lock = RECEIVER.lock();
                    (*lock).clone()
                };

                let boxed_metering_event: Box<dyn MeteringEvent> = Box::new(self);

                if let Err(error) = receiver.send(boxed_metering_event, maybe_current_span).await {
                    tracing::error!("Unable to send meter event: {error}");
                }
            }
        }

        /// A blanket-impl of the `submit` method for all metering events.
        #[async_trait::async_trait]
        impl<T> SubmitExt for T
        where
            T: MeteringEvent + Send + 'static,
        {
            async fn submit(self) {
                let maybe_current_span = Some(tracing::Span::current());
                let receiver: Box<
                    dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>,
                > = {
                    let lock = RECEIVER.lock();
                    (*lock).clone_box()
                };
                let boxed_metering_event: Box<dyn MeteringEvent> = Box::new(self);
                if let Err(error) = receiver.send(boxed_metering_event, maybe_current_span).await {
                    tracing::error!("Unable to send meter event: {error}");
                }
            }
        }

        thread_local! {
            /// The thread-local event stack in which metering events are stored.
            static EVENT_STACK: std::cell::RefCell<Vec<(std::any::TypeId, Box<dyn MeteringEvent>)>> = std::cell::RefCell::new(Vec::new());
        }

        /// A zero-sized struct used to implement RAII for metering events.
        pub struct MeteringEventGuard;

        /// We implement drop for the guard such that metering events are dropped when they fall out of scope.
        impl Drop for MeteringEventGuard {
            fn drop(&mut self) {
                if let Some(dropped_event) = EVENT_STACK.with(|event_stack| event_stack.borrow_mut().pop())
                {
                    tracing::warn!(
                        "Dropping event because it is now out of scope: {:?}",
                        dropped_event
                    );
                }
            }
        }

        thread_local! {
            /// A thread-local pointer to an empty metering event such that if the stack is empty
            /// method invocations won't fail.
            static BLANK_METERING_EVENT_POINTER: *mut dyn MeteringEvent = {
                let boxed_blank_metering_event = Box::new(BlankMeteringEvent);
                Box::into_raw(boxed_blank_metering_event) as *mut dyn MeteringEvent
            };
        }

        /// A zero-sized metering event to use in case of the stack being empty.
        struct BlankMeteringEvent;

        /// We implement debug so that the metering event can be sent to the default receiver.
        impl std::fmt::Debug for BlankMeteringEvent {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "BlankMeteringEvent")
            }
        }

        /// The blank metering event has no custom mutators, so everything is a no-op.
        impl MeteringEvent for BlankMeteringEvent {}

        /// Creates a metering event of type `E` and pushes it onto the stack.
        pub fn create<E: MeteringEvent>(metering_event: E) -> MeteringEventGuard {
            let type_id = std::any::TypeId::of::<E>();
            let boxed_metering_event: Box<dyn MeteringEvent> =
                Box::new(metering_event);
            EVENT_STACK.with(|event_stack| {
                event_stack
                    .borrow_mut()
                    .push((type_id, boxed_metering_event));
            });
            MeteringEventGuard
        }

        /// Returns a pointer to the metering event at the top of the stack.
        pub fn current() -> &'static mut dyn MeteringEvent {
            if let Some(raw_metering_event_pointer) = EVENT_STACK.with(|event_stack| {
                let mut mutable_event_stack = event_stack.borrow_mut();
                if let Some((_, boxed_metering_event)) = mutable_event_stack.last_mut() {
                    let raw_pointer: *mut dyn MeteringEvent =
                        &mut **boxed_metering_event as *mut dyn MeteringEvent;
                    Some(raw_pointer)
                } else {
                    None
                }
            }) {
                unsafe { &mut *raw_metering_event_pointer }
            } else {
                BLANK_METERING_EVENT_POINTER.with(|pointer| unsafe { &mut *(*pointer) })
            }
        }

        /// Checks if the top event on the stack is of type `E`. If so, the event is removed from the stack
        /// and returned to the caller. If not, `None` is returned.
        pub fn close<E: MeteringEvent>() -> Option<E> {
            EVENT_STACK.with(|event_stack| {
                let mut vec = event_stack.borrow_mut();
                if let Some((type_id, _)) = vec.last() {
                    if *type_id == std::any::TypeId::of::<E>() {
                        let (_type_id, boxed_generic_metering_event) = vec.pop().unwrap();
                        let raw_generic_metering_event: *mut dyn MeteringEvent =
                            Box::into_raw(boxed_generic_metering_event);
                        let raw_metering_event: *mut E = raw_generic_metering_event as *mut E;
                        let boxed_metering_event: Box<E> = unsafe { Box::from_raw(raw_metering_event) };
                        return Some(*boxed_metering_event);
                    }
                }
                None
            })
        }

        /// A trait that allows futures to be “thread-hopping-aware.”
        /// When a user calls `.metered(guard)`, we immediately take the top event off the current stack,
        /// stash it in the returned `MeteredFuture`, and then on the first `poll()` push it into whichever
        /// thread is currently running the future. Finally, when that `MeteredFuture` is dropped at the end,
        /// its `MeteringEventGuard` drop will pop from that thread’s stack.
        pub trait MeteredFutureExt: std::future::Future + Sized {
            fn metered(self, guard: MeteringEventGuard) -> MeteredFuture<Self> {
                let (moved_type_id, moved_boxed) = EVENT_STACK.with(|event_stack| {
                    event_stack
                        .borrow_mut()
                        .pop()
                        .expect("`.metered()` called but no MeteringEventGuard had pushed any event")
                });

                MeteredFuture {
                    inner: self,
                    moved_type_id: Some(moved_type_id),
                    moved_boxed: Some(moved_boxed),
                    pushed: false,
                    _guard: guard,
                }
            }
        }

        /// Blanket-impl of the `MeteredFutureExt` trait for futures.
        impl<F: std::future::Future> MeteredFutureExt for F {}

        /// The struct that holds the inner future for metered futures. Once the future is actually polled
        /// on the new thread, we do a one-time `push` there. If the future is never polled (or ends immediately),
        /// the `Drop` of `_guard` will pop it from whichever `EVENT_STACK` still sees it.
        pub struct MeteredFuture<F: std::future::Future> {
            inner: F,
            moved_type_id: Option<std::any::TypeId>,
            moved_boxed: Option<Box<dyn MeteringEvent>>,
            pushed: bool,
            _guard: MeteringEventGuard,
        }


        impl<F: std::future::Future> std::future::Future for MeteredFuture<F> {
            type Output = F::Output;

            fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                let this: &mut MeteredFuture<F> = unsafe { self.get_unchecked_mut() };

                if !this.pushed {
                    if let (Some(type_id), Some(boxed_evt)) =
                        (this.moved_type_id.take(), this.moved_boxed.take())
                    {
                        EVENT_STACK.with(|stack| {
                            stack.borrow_mut().push((type_id, boxed_evt));
                        });
                        this.pushed = true;
                    }
                }

                let inner_pin: std::pin::Pin<&mut F> = unsafe { std::pin::Pin::new_unchecked(&mut this.inner) };
                inner_pin.poll(cx)
            }
        }

        // We rely on the guard's Drop to pop the event when either the future finishes and is dropped,
        // or someone explicitly calls `close::<E>()`.
        impl<F: std::future::Future + Unpin> Unpin for MeteredFuture<F> {}
    })
}
