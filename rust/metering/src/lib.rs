use std::{
    any::{Any, TypeId},
    cell::RefCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::fmt::Debug;
use thiserror::Error;

pub use chroma_metering_macros::*;
pub use chroma_metering_registry::*;

#[derive(Debug, Error)]
pub enum MeteringError {
    #[error("Receiver already initialized")]
    ReceiverAlreadyInitializedError,
}

pub trait MeteringEvent: Debug + Any + Send + 'static {
    chroma_metering_macros::generate_base_mutators! {}
}

pub type MutatorFn = fn(&mut Box<dyn MeteringEvent>, dyn Any);

#[derive(Clone, Debug)]
pub struct DefaultReceiver;

#[async_trait]
impl chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>> for DefaultReceiver {
    async fn send(
        &self,
        message: Box<dyn MeteringEvent>,
        tracing_context: Option<tracing::Span>,
    ) -> Result<(), chroma_system::ChannelError> {
        if let Some(span) = tracing_context {
            println!("[meter] span={:?} event={:?}", span, message);
        } else {
            println!("[meter] event={:?}", message);
        }
        Ok(())
    }
}

static RECEIVER: Lazy<Mutex<Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>>>> =
    Lazy::new(|| Mutex::new(Box::new(DefaultReceiver)));

pub fn register_receiver(
    receiver: Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>>,
) -> Result<(), MeteringError> {
    let mut receiver_slot = RECEIVER.lock();
    *receiver_slot = receiver;
    Ok(())
}

#[async_trait]
pub trait SubmitExt: MeteringEvent + Sized + Send {
    async fn submit(self) {
        let span_opt = Some(tracing::Span::current());

        let handler: Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>> = {
            let lock = RECEIVER.lock();
            (*lock).clone()
        };

        let boxed_evt: Box<dyn MeteringEvent> = Box::new(self);

        if let Err(err) = handler.send(boxed_evt, span_opt).await {
            tracing::error!("Unable to send meter event: {err}");
        }
    }
}

#[async_trait]
impl<T> SubmitExt for T
where
    T: MeteringEvent + Send + 'static,
{
    async fn submit(self) {
        let span_opt = Some(tracing::Span::current());
        let handler: Box<dyn chroma_system::ReceiverForMessage<Box<dyn MeteringEvent>>> = {
            let lock = RECEIVER.lock();
            (*lock).clone_box()
        };
        let boxed_evt: Box<dyn MeteringEvent> = Box::new(self);
        if let Err(err) = handler.send(boxed_evt, span_opt).await {
            tracing::error!("Unable to send meter event: {err}");
        }
    }
}

thread_local! {
    static EVENT_STACK: RefCell<Vec<(TypeId, Box<dyn MeteringEvent>)>> = RefCell::new(Vec::new());
}

pub struct MeteringEventGuard;
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

pub fn create<E: MeteringEvent>(ev: E) -> MeteringEventGuard {
    let type_id = TypeId::of::<E>();
    let boxed_evt: Box<dyn MeteringEvent> = Box::new(ev);
    EVENT_STACK.with(|stack| {
        stack.borrow_mut().push((type_id, boxed_evt));
    });
    MeteringEventGuard
}

thread_local! {
    static THREAD_NOOP_PTR: *mut dyn MeteringEvent = {
        // Allocate one `Box<NoopMeteringEvent>` per thread.
        let boxed = Box::new(NoopMeteringEvent);
        Box::into_raw(boxed) as *mut dyn MeteringEvent
    };
}

struct NoopMeteringEvent;

impl Debug for NoopMeteringEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NoopMeteringEvent")
    }
}

impl MeteringEvent for NoopMeteringEvent {}

pub fn current() -> &'static mut dyn MeteringEvent {
    // 1) Try to get a mutable reference to the top of the stack:
    if let Some(raw_ptr) = EVENT_STACK.with(|stack| {
        let mut vec = stack.borrow_mut();
        // Use last_mut() instead of last():
        if let Some((_, boxed_evt)) = vec.last_mut() {
            // boxed_evt: &mut Box<dyn MeteringEvent>
            let raw: *mut dyn MeteringEvent = &mut **boxed_evt as *mut dyn MeteringEvent;
            Some(raw)
        } else {
            None
        }
    }) {
        // SAFETY: box still lives in the Vec, so &mut is valid
        unsafe { &mut *raw_ptr }
    } else {
        // 2) Fallback: thread‐local no–op
        THREAD_NOOP_PTR.with(|p| {
            // SAFETY: each thread has its own Noop pointer, so returning &mut *p is safe
            unsafe { &mut *(*p) }
        })
    }
}

/// Pops the top‐of‐stack if it is exactly an E, and returns `Some(E)`. Otherwise returns `None`.
pub fn close<E: MeteringEvent>() -> Option<E> {
    EVENT_STACK.with(|stack| {
        let mut vec = stack.borrow_mut();
        if let Some((type_id, _boxed_evt)) = vec.last() {
            if *type_id == TypeId::of::<E>() {
                // Pop off (type_id, boxed_evt)
                let (_type_id, boxed_any) = vec.pop().unwrap();
                // Convert into raw pointer:
                let raw_evt: *mut dyn MeteringEvent = Box::into_raw(boxed_any);
                // Cast `*mut dyn MeteringEvent` → `*mut E` (safe because TypeId matched).
                let raw_e: *mut E = raw_evt as *mut E;
                // Reconstruct Box<E> and return the inner E
                let boxed_e: Box<E> = unsafe { Box::from_raw(raw_e) };
                return Some(*boxed_e);
            }
        }
        None
    })
}

pub trait MeteredFutureExt: Future + Sized {
    fn metered(self, _metering_event_guard: MeteringEventGuard) -> MeteredFuture<Self> {
        MeteredFuture { inner: self }
    }
}

impl<F: Future> MeteredFutureExt for F {}

pub struct MeteredFuture<F: Future> {
    inner: F,
}

impl<F: Future> Future for MeteredFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let inner_future = unsafe {
            self.as_mut()
                .map_unchecked_mut(|metered_future| &mut metered_future.inner)
        };
        inner_future.poll(context)
    }
}

impl<F: Future + Unpin> Unpin for MeteredFuture<F> {}
