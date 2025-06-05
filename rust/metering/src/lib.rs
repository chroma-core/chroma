// use std::{
//     any::TypeId,
//     cell::RefCell,
//     future::Future,
//     pin::Pin,
//     task::{Context, Poll},
// };

// use async_trait::async_trait;
// use once_cell::sync::Lazy;
// use parking_lot::Mutex;
// use std::fmt::Debug;

// pub use chroma_metering_core;
// pub use chroma_metering_macros::{attribute, event};

// /// The default receiver registered in the library.
// #[derive(Clone, Debug)]
// pub struct DefaultReceiver;

// /// The default receiver simply prints out the metering events submitted to it.
// #[async_trait]
// impl chroma_system::ReceiverForMessage<Box<dyn chroma_metering_core::MeteringEvent>>
//     for DefaultReceiver
// {
//     async fn send(
//         &self,
//         message: Box<dyn chroma_metering_core::MeteringEvent>,
//         tracing_context: Option<tracing::Span>,
//     ) -> Result<(), chroma_system::ChannelError> {
//         if let Some(span) = tracing_context {
//             println!("[chroma_metering] span={:?} event={:?}", span, message);
//         } else {
//             println!("[chroma_metering] event={:?}", message);
//         }
//         Ok(())
//     }
// }

// /// The storage slot for the registered receiver.
// static RECEIVER: Lazy<
//     Mutex<Box<dyn chroma_system::ReceiverForMessage<Box<dyn chroma_metering_core::MeteringEvent>>>>,
// > = Lazy::new(|| Mutex::new(Box::new(DefaultReceiver)));

// /// Allows library users to register their own receivers.
// pub fn register_receiver(
//     receiver: Box<
//         dyn chroma_system::ReceiverForMessage<Box<dyn chroma_metering_core::MeteringEvent>>,
//     >,
// ) {
//     let mut receiver_slot = RECEIVER.lock();
//     *receiver_slot = receiver;
// }

// /// A trait containing a `submit` method to send metering events to the registered receiver.
// #[async_trait]
// pub trait SubmitExt: chroma_metering_core::MeteringEvent + Sized + Send {
//     async fn submit(self) {
//         let span_opt = Some(tracing::Span::current());

//         let handler: Box<
//             dyn chroma_system::ReceiverForMessage<Box<dyn chroma_metering_core::MeteringEvent>>,
//         > = {
//             let lock = RECEIVER.lock();
//             (*lock).clone()
//         };

//         let boxed_evt: Box<dyn chroma_metering_core::MeteringEvent> = Box::new(self);

//         if let Err(err) = handler.send(boxed_evt, span_opt).await {
//             tracing::error!("Unable to send meter event: {err}");
//         }
//     }
// }

// /// A blanket-impl of the `submit` method for all metering events.
// #[async_trait]
// impl<T> SubmitExt for T
// where
//     T: chroma_metering_core::MeteringEvent + Send + 'static,
// {
//     async fn submit(self) {
//         let span_opt = Some(tracing::Span::current());
//         let handler: Box<
//             dyn chroma_system::ReceiverForMessage<Box<dyn chroma_metering_core::MeteringEvent>>,
//         > = {
//             let lock = RECEIVER.lock();
//             (*lock).clone_box()
//         };
//         let boxed_evt: Box<dyn chroma_metering_core::MeteringEvent> = Box::new(self);
//         if let Err(err) = handler.send(boxed_evt, span_opt).await {
//             tracing::error!("Unable to send meter event: {err}");
//         }
//     }
// }

// thread_local! {
//     /// The thread-local event stack in which metering events are stored.
//     static EVENT_STACK: RefCell<Vec<(TypeId, Box<dyn chroma_metering_core::MeteringEvent>)>> = RefCell::new(Vec::new());
// }

// /// A zero-sized struct used to implement RAII for metering events.
// pub struct MeteringEventGuard;

// /// We implement drop for the guard such that metering events are dropped when they fall out of scope.
// impl Drop for MeteringEventGuard {
//     fn drop(&mut self) {
//         if let Some(dropped_event) = EVENT_STACK.with(|event_stack| event_stack.borrow_mut().pop())
//         {
//             tracing::warn!(
//                 "Dropping event because it is now out of scope: {:?}",
//                 dropped_event
//             );
//         }
//     }
// }

// /// Creates a metering event of type `E` and pushes it onto the stack.
// pub fn create<E: chroma_metering_core::MeteringEvent>(metering_event: E) -> MeteringEventGuard {
//     let type_id = TypeId::of::<E>();
//     let boxed_metering_event: Box<dyn chroma_metering_core::MeteringEvent> =
//         Box::new(metering_event);
//     EVENT_STACK.with(|event_stack| {
//         event_stack
//             .borrow_mut()
//             .push((type_id, boxed_metering_event));
//     });
//     MeteringEventGuard
// }

// thread_local! {
//     /// A thread-local pointer to an empty metering event such that if the stack is empty
//     /// method invocations won't fail.
//     static BLANK_METERING_EVENT_POINTER: *mut dyn chroma_metering_core::MeteringEvent = {
//         let boxed_blank_metering_event = Box::new(BlankMeteringEvent);
//         Box::into_raw(boxed_blank_metering_event) as *mut dyn chroma_metering_core::MeteringEvent
//     };
// }

// /// A zero-sized metering event to use in case of the stack being empty.
// struct BlankMeteringEvent;

// /// We implement debug so that the metering event can be sent to the default receiver.
// impl Debug for BlankMeteringEvent {
//     fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(formatter, "BlankMeteringEvent")
//     }
// }

// /// The blank metering event has no custom mutators, so everything is a no-op.
// impl chroma_metering_core::MeteringEvent for BlankMeteringEvent {}

// /// Returns a pointer to the metering event at the top of the stack.
// pub fn current() -> &'static mut dyn chroma_metering_core::MeteringEvent {
//     if let Some(raw_ptr) = EVENT_STACK.with(|event_stack| {
//         let mut vec = event_stack.borrow_mut();
//         if let Some((_, boxed_evt)) = vec.last_mut() {
//             let raw: *mut dyn chroma_metering_core::MeteringEvent =
//                 &mut **boxed_evt as *mut dyn chroma_metering_core::MeteringEvent;
//             Some(raw)
//         } else {
//             None
//         }
//     }) {
//         unsafe { &mut *raw_ptr }
//     } else {
//         BLANK_METERING_EVENT_POINTER.with(|p| unsafe { &mut *(*p) })
//     }
// }

// /// Checks if the top event on the stack is of type `E`. If so, the event is removed from the stack
// /// and returned to the caller. If not, `None` is returned.
// pub fn close<E: chroma_metering_core::MeteringEvent>() -> Option<E> {
//     EVENT_STACK.with(|event_stack| {
//         let mut vec = event_stack.borrow_mut();
//         if let Some((type_id, _boxed_evt)) = vec.last() {
//             if *type_id == TypeId::of::<E>() {
//                 let (_type_id, boxed_any) = vec.pop().unwrap();
//                 let raw_evt: *mut dyn chroma_metering_core::MeteringEvent =
//                     Box::into_raw(boxed_any);
//                 let raw_e: *mut E = raw_evt as *mut E;
//                 let boxed_e: Box<E> = unsafe { Box::from_raw(raw_e) };
//                 return Some(*boxed_e);
//             }
//         }
//         None
//     })
// }

// /// A trait that allows futures to be metered to pass events between async contexts.
// pub trait MeteredFutureExt: Future + Sized {
//     fn metered(self, _metering_event_guard: MeteringEventGuard) -> MeteredFuture<Self> {
//         MeteredFuture { inner: self }
//     }
// }

// /// Blanket-impl of the `MeteredFutureExt` trait for futures.
// impl<F: Future> MeteredFutureExt for F {}

// /// The struct that holds the inner future for metered futures.
// pub struct MeteredFuture<F: Future> {
//     inner: F,
// }

// /// Implementation of the `Future` trait for `MeteredFuture`.
// impl<F: Future> Future for MeteredFuture<F> {
//     type Output = F::Output;

//     fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
//         let inner_future = unsafe {
//             self.as_mut()
//                 .map_unchecked_mut(|metered_future| &mut metered_future.inner)
//         };
//         inner_future.poll(context)
//     }
// }

// /// Implementation of `Unpin` for metered future.
// impl<F: Future + Unpin> Unpin for MeteredFuture<F> {}

#[cfg(test)]
mod tests {
    chroma_metering_macros::initialize_metering! {
        #[attribute(name = "my_test_attribute")]
        type MyTestAttribute = Option<u64>;

        #[event]
        struct MyTestEvent {
            test_constant_field: String,
            #[field(attribute = "my_test_attribute", mutator = "my_test_mutator")]
            test_annotated_field: MyTestAttribute,
        }
    }

    fn my_test_mutator(event: &mut MyTestEvent, value: MyTestAttribute) {
        event.test_annotated_field = value;
    }

    fn test_register_custom_receiver() {}

    #[tokio::test]
    async fn test_single_metering_event() {}

    #[tokio::test]
    async fn test_many_metering_events_uniform_type_single_context() {}

    #[tokio::test]
    async fn test_many_metering_events_varying_type_single_context() {}

    #[tokio::test]
    async fn test_many_metering_events_uniform_type_multi_context() {}

    #[tokio::test]
    async fn test_many_metering_events_varying_type_multi_context() {}

    #[tokio::test]
    async fn test_metered_future() {}
}
