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

        /// A global map stores a map of thread IDs two stacks of metering events.
        static EVENT_STACKS: once_cell::sync::Lazy<
            parking_lot::Mutex<std::collections::HashMap<std::thread::ThreadId, Vec<(std::any::TypeId, Box<dyn MeteringEvent>)>>>> =
            once_cell::sync::Lazy::new(|| parking_lot::Mutex::new(std::collections::HashMap::new()));

        /// Gets a mutable reference to the current thread's event stack.
        fn get_stack_for_current_thread<'a>(
            event_stacks: &'a mut std::collections::HashMap<std::thread::ThreadId, Vec<(std::any::TypeId, Box<dyn MeteringEvent>)>>,
        ) -> &'a mut Vec<(std::any::TypeId, Box<dyn MeteringEvent>)> {
            let thread_id = std::thread::current().id();
            event_stacks.entry(thread_id).or_default()
        }

        /// Pops the top entry (TypeId, Box<dyn MeteringEvent>) from the current thread and returns it.
        /// If no event exists, returns `None`.
        fn pop_from_thread(thread_id: std::thread::ThreadId) -> Option<(std::any::TypeId, Box<dyn MeteringEvent>)> {
            let mut event_stacks = EVENT_STACKS.lock();
            if let Some(event_stack) = event_stacks.get_mut(&thread_id) {
                event_stack.pop()
            } else {
                None
            }
        }

        /// Pushes an entry (TypeId, Box<dyn MeteringEvent>) onto the current thread's stack.
        fn push_to_thread(thread_id: std::thread::ThreadId, entry: (std::any::TypeId, Box<dyn MeteringEvent>)) {
            let mut event_stacks = EVENT_STACKS.lock();
            let event_stack = event_stacks.entry(thread_id).or_default();
            event_stack.push(entry);
        }

        /// Peek at the top of the current thread’s stack, returning a raw pointer if an event is present;
        /// else returns `None`.  If `None`, callers should use a `BlankMeteringEvent`
        fn peek_current_thread_raw() -> Option<*mut dyn MeteringEvent> {
            let mut event_stacks = EVENT_STACKS.lock();
            let thread_id = std::thread::current().id();
            if let Some(event_stack) = event_stacks.get_mut(&thread_id) {
                if let Some((_, boxed_metering_event)) = event_stack.last_mut() {
                    let raw_pointer: *mut dyn MeteringEvent = &mut **boxed_metering_event;
                    return Some(raw_pointer);
                }
            }
            None
        }

        thread_local! {
            /// A thread-local pointer to a  blank event to return if the stack is empty
            static BLANK_METERING_EVENT_POINTER: *mut dyn MeteringEvent = {
                let boxed_blank_metering_event = Box::new(BlankMeteringEvent);
                Box::into_raw(boxed_blank_metering_event) as *mut dyn MeteringEvent
            };
        }

        /// A zero-sized metering event if a thread's stack is empty.
        struct BlankMeteringEvent;
        impl std::fmt::Debug for BlankMeteringEvent {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "BlankMeteringEvent")
            }
        }
        impl MeteringEvent for BlankMeteringEvent {}

        /// Creates a metering event of type `E` (boxed), and immediately pushes it onto the current thread’s stack.
        /// Returns a `MeteringEventGuard` whose job is to return that same event to its previous thread.
        pub fn create<E: MeteringEvent>(metering_event: E) -> MeteringEventGuard {
            let metering_event_type_id = std::any::TypeId::of::<E>();
            let boxed_metering_event = Box::new(metering_event) as Box<dyn MeteringEvent>;
            let thread_id = std::thread::current().id();

            // Push an event onto the current thread's event stack
            {
                let mut event_stacks = EVENT_STACKS.lock();
                let event_stack = event_stacks.entry(thread_id).or_default();
                event_stack.push((metering_event_type_id, boxed_metering_event));
            }

            // Return a guard that remembers we pushed onto thread `thread_id`
            MeteringEventGuard { previous_thread_id: thread_id }
        }

        /// Return a mutable reference to the metering event at the top of the current thread’s stack,
        /// or to a blank metering event if no events are present.
        pub fn current() -> &'static mut dyn MeteringEvent {
            if let Some(raw_pointer) = peek_current_thread_raw() {
                // SAFETY(c-gamble): `raw_pointer` came from a boxed MeteringEvent in this thread’s stack.
                unsafe { &mut *raw_pointer }
            } else {
                // No event on this thread. Use a blank event.
                BLANK_METERING_EVENT_POINTER.with(|ptr| unsafe { &mut *(*ptr) })
            }
        }

        /// Pops the top event from the current thread’s stack if it is of type `E`. Otherwise, returns `None`.
        pub fn close<E: MeteringEvent>() -> Option<E> {
            let metering_event_type_id = std::any::TypeId::of::<E>();
            let thread_id = std::thread::current().id();

            let mut event_stacks = EVENT_STACKS.lock();
            if let Some(event_stack) = event_stacks.get_mut(&thread_id) {
                if let Some((top_type_id, _)) = event_stack.last() {
                    if *top_type_id == metering_event_type_id {
                        // It matches, so pop it and downcast
                        let (_, boxed_generic_metering_event) = event_stack.pop().unwrap();
                        let generic_metering_event_raw_pointer: *mut dyn MeteringEvent = Box::into_raw(boxed_generic_metering_event);
                        let concrete_metering_event_raw_pointer: *mut E = generic_metering_event_raw_pointer as *mut E;
                        // SAFETY(c-gamble): We know it was an E.
                        let boxed_metering_event: Box<E> = unsafe { Box::from_raw(concrete_metering_event_raw_pointer) };
                        return Some(*boxed_metering_event);
                    }
                }
            }
            None
        }

        /// A guard that knows the previous thread ID for a metering event.
        /// When dropped, it will take exactly one event off wherever it currently lives
        /// and move it back onto the previous thread's stack.
        pub struct MeteringEventGuard {
            previous_thread_id: std::thread::ThreadId,
        }

        impl Drop for MeteringEventGuard {
            fn drop(&mut self) {
                // Pop from the thread is currently running this Drop
                let current_thread_id = std::thread::current().id();
                if let Some((metering_event_type_id, boxed_metering_event)) = pop_from_thread(current_thread_id) {
                    push_to_thread(self.previous_thread_id, (metering_event_type_id, boxed_metering_event));
                }
                // If there was no event on the current thread’s stack, do nothing
            }
        }

        /// A trait for futures to allow moving metering events between threads.
        pub trait MeteredFutureExt: std::future::Future + Sized {
            fn metered(self, metering_event_guard: MeteringEventGuard) -> MeteredFuture<Self> {
                // Pop the top event from the previous thread’s stack immediately:
                let previous_thread_id = metering_event_guard.previous_thread_id;
                let (moved_metering_event_type_id, moved_boxed_metering_event) = {
                    pop_from_thread(previous_thread_id).unwrap_or_else(|| {
                        panic!(
                            "`.metered()` called but no MeteringEvent was on thread {:?}",
                            previous_thread_id
                        )
                    })
                };

                MeteredFuture {
                    inner: self,
                    previous_thread_id,
                    moved_metering_event_type_id: Some(moved_metering_event_type_id),
                    moved_boxed_metering_event: Some(moved_boxed_metering_event),
                    new_thread_id: None,
                }
            }
        }
        impl<F: std::future::Future> MeteredFutureExt for F {}

        /// The wrapper that, on its first poll, pushes an event into the new thread’s stack,
        /// and on Drop (when the future ends or is cancelled) pops from the new and puts it back on its previous threads.
        pub struct MeteredFuture<F: std::future::Future> {
            inner: F,
            previous_thread_id: std::thread::ThreadId,
            moved_metering_event_type_id: Option<std::any::TypeId>,
            moved_boxed_metering_event: Option<Box<dyn MeteringEvent>>,
            new_thread_id: Option<std::thread::ThreadId>,
        }

        impl<F: std::future::Future> std::future::Future for MeteredFuture<F> {
            type Output = F::Output;

            fn poll(self: std::pin::Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                // Project `Pin<&mut MeteredFuture<F>>` to `&mut MeteredFuture<F>`
                let metered_future: &mut MeteredFuture<F> = unsafe { self.get_unchecked_mut() };

                // On the very first poll, if we have an event stashed that we moved, push it into the new thread’s stack:
                if metered_future.new_thread_id.is_none() {
                    // We haven’t yet done the previous -> new transfer
                    if let (Some(thread_id), Some(boxed_metering_event)) =
                        (metered_future.moved_metering_event_type_id.take(), metered_future.moved_boxed_metering_event.take())
                    {
                        // Record which thread is now the the new thread
                        let current_thread_id = std::thread::current().id();
                        metered_future.new_thread_id = Some(current_thread_id);
                        push_to_thread(current_thread_id, (thread_id, boxed_metering_event));
                    }
                }

                // Delegate to the inner future
                let inner_future: std::pin::Pin<&mut F> = unsafe { std::pin::Pin::new_unchecked(&mut metered_future.inner) };
                inner_future.poll(context)
            }
        }

        /// MeteredFuture<F> is Unpin whenever F: Unpin
        impl<F: std::future::Future + Unpin> Unpin for MeteredFuture<F> {}
    })
}
