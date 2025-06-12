/// The base trait that is programmatically implemented for all user-defined metering
/// contexts
pub trait MeteringContext:
    ::std::fmt::Debug + ::std::any::Any + ::std::marker::Send + ::std::marker::Sync + 'static
{
    fn clone_box(&self) -> Box<dyn MeteringContext>;

    fn as_any(&self) -> &dyn ::std::any::Any;
}

/// An implementation of `Clone` for boxed trait objects of `MeteringContext`
impl Clone for Box<dyn MeteringContext> {
    fn clone(&self) -> Box<dyn MeteringContext> {
        self.clone_box()
    }
}

/// A blank metering context to use when there is no active metering context
#[derive(::std::fmt::Debug, Clone)]
pub struct BlankMeteringContext;

/// We implement the `MeteringContext` trait for the blank metering context so it can be represented
/// in the same way that a user-defined metering context would be internally
impl MeteringContext for BlankMeteringContext {
    fn clone_box(&self) -> Box<dyn MeteringContext> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn ::std::any::Any {
        self
    }
}

/// A runtime error that occurs in the metering library
#[derive(Debug, thiserror::Error)]
pub enum MeteringError {
    #[error("The metering context receiver has already been initialized")]
    ReceiverAlreadyInitializedError,
    #[error("The mutex handleing the active metering context was poisoned")]
    PoisonedMutexError,
    #[error("Requested type is not the same as the active context's type on this thread")]
    TypeMismatchError,
    #[error(
        "Failed to downcast context from std::any::Any to provided type, despite type IDs matching"
    )]
    DowncastError,
}

/// A safe reference to a metering context to share between threads
#[derive(Clone, std::fmt::Debug)]
struct MeteringContextContainer(::std::sync::Arc<Box<dyn MeteringContext>>);

/// The default value for `MeteringContextContainer` is a `BlankMeteringContext`
impl ::std::default::Default for MeteringContextContainer {
    fn default() -> Self {
        MeteringContextContainer(::std::sync::Arc::new(Box::new(BlankMeteringContext)))
    }
}

/// `enter` and `exit` methods to enable metering in synchronous cases
impl MeteringContextContainer {
    fn enter(&self) {
        ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
            cell.replace(self.clone());
        });
    }

    fn exit(&self) {
        ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
            cell.replace(MeteringContextContainer::default());
        });
    }
}
// Thread-local storage for the active metering context
::std::thread_local! {
    static ACTIVE_METERING_CONTEXT_CONTAINER: ::std::cell::RefCell<MeteringContextContainer> =
        ::std::cell::RefCell::new(MeteringContextContainer::default());
}

/// Creates a metering context of type `C` and returns a handle
pub fn create<C: MeteringContext>(metering_context: C) -> MeteringContextHandle {
    let metering_context_type_id = ::std::any::TypeId::of::<C>();
    let shared_boxed_metering_context = ::std::sync::Arc::new(::std::sync::Mutex::new(Box::new(
        metering_context,
    )
        as Box<dyn MeteringContext>));

    // ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| {
    //     slot.replace(OldMeteringContextContainer {
    //         shared_boxed_metering_context: shared_boxed_metering_context.clone(),
    //         metering_context_type_id,
    //     });
    // });

    MeteringContextHandle {
        inner_shared_boxed_metering_context: shared_boxed_metering_context,
        inner_metering_context_type_id: metering_context_type_id,
    }
}

/// Allows users to specify a closure to invoke on the current thread's active metering context.
/// If no context is active, this will be a no-op because the mutation will be applied to
/// `BlankMeteringContext`
pub fn with_current(mutator: impl FnOnce(&mut dyn MeteringContext)) {
    ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| {
        let active_metering_context_container = slot.borrow();
        if let Ok(mut shared_boxed_metering_context) = active_metering_context_container
            .shared_boxed_metering_context
            .lock()
        {
            mutator(&mut **shared_boxed_metering_context);
        };
    });
}

/// Closes the current thread's metering context if it is of type `C`, otherwise returns an error
pub fn close<C: MeteringContext + Clone>() -> Result<C, MeteringError> {
    ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| {
        let mut active_metering_context_container = slot.borrow_mut();

        let mut guard = active_metering_context_container
            .shared_boxed_metering_context
            .lock()
            .map_err(|_| MeteringError::PoisonedMutexError)?;

        if !guard.as_any().is::<C>() {
            return Err(MeteringError::TypeMismatchError);
        }

        let metering_context = guard
            .as_any()
            .downcast_ref::<C>()
            .map(Clone::clone)
            .ok_or(MeteringError::DowncastError)?;

        *guard = Box::new(BlankMeteringContext);

        drop(guard);

        active_metering_context_container.metering_context_type_id =
            ::std::any::TypeId::of::<BlankMeteringContext>();

        active_metering_context_container.shared_boxed_metering_context =
            ::std::sync::Arc::new(::std::sync::Mutex::new(Box::new(BlankMeteringContext)));

        Ok(metering_context)
    })
}

/// A handle that stores a metering context and its type ID
#[derive(std::fmt::Debug)]
pub struct MeteringContextHandle {
    pub inner_shared_boxed_metering_context: MeteringContextContainer,
    inner_metering_context_type_id: ::std::any::TypeId,
}

/// A trait that allows futures to be `metered`, similar to how `tracing` enables futures to be
/// `instrumented`
pub trait MeteredFutureExt: ::std::future::Future + Sized {
    fn metered(self, metering_context_handle: MeteringContextContainer) -> MeteredFuture<Self> {
        MeteredFuture {
            inner_future: self,
            metering_context_handle,
        }
    }
}

pub fn get_current() -> MeteringContextHandle {
    ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| {
        let active = slot.borrow();
        return MeteringContextHandle {
            inner_shared_boxed_metering_context: active.shared_boxed_metering_context.clone(),
            inner_metering_context_type_id: active.metering_context_type_id,
        };
    })
}

/// A blanket implementation of `metered` for all futures
impl<F: ::std::future::Future> MeteredFutureExt for F {}

/// Similar to `tracing::Instrumented`, this wraps a future and stores the
/// active metering context in the thread's local storage
#[pin_project::pin_project]
pub struct MeteredFuture<F: ::std::future::Future> {
    #[pin]
    inner_future: F,
    metering_context_handle: ::std::sync::Arc<MeteringContextHandle>,
}

/// Handles setting the current thread's active metering context when it is polled and
/// unsetting it after the poll is complete
impl<F: ::std::future::Future> ::std::future::Future for MeteredFuture<F> {
    type Output = F::Output;

    fn poll(
        self: ::std::pin::Pin<&mut Self>,
        context: &mut ::std::task::Context<'_>,
    ) -> ::std::task::Poll<Self::Output> {
        let metered_future = self.project();

        let new_container = OldMeteringContextContainer {
            shared_boxed_metering_context: metered_future
                .metering_context_handle
                .inner_shared_boxed_metering_context
                .clone(),
            metering_context_type_id: metered_future
                .metering_context_handle
                .inner_metering_context_type_id,
        };

        let previous_container =
            ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| slot.replace(new_container));

        let output = metered_future.inner_future.poll(context);

        ACTIVE_METERING_CONTEXT_CONTAINER.with(|slot| {
            slot.replace(previous_container);
        });

        output
    }
}

/// A global variable that stores the receiver to which metering contexts are sent
/// when they are submitted.
static RECEIVER: ::std::sync::OnceLock<
    Box<dyn ::chroma_system::ReceiverForMessage<Box<dyn MeteringContext>>>,
> = ::std::sync::OnceLock::new();

/// Initialize a custom receiver that implements `chroma_system::ReceiverForMessage`.
/// Returns a void result if successful, else a `ReceiverAlreadyInitializedError` if
/// the receiver has already been initialized.
pub fn init_receiver(
    receiver: Box<dyn ::chroma_system::ReceiverForMessage<Box<dyn MeteringContext>>>,
) -> Result<(), MeteringError> {
    if RECEIVER.set(receiver).is_err() {
        return Err(MeteringError::ReceiverAlreadyInitializedError);
    }
    Ok(())
}

/// A trait that defines a `submit` function that sends metering contexts to their receiver.
/// Emits an error trace if sending is unsuccessful.
#[async_trait::async_trait]
pub trait SubmitExt: MeteringContext + Sized {
    async fn submit(self) {
        if let Some(receiver) = RECEIVER.get() {
            if let Err(error) = receiver
                .send(Box::new(self), Some(::tracing::Span::current()))
                .await
            {
                ::tracing::error!("Unable to send metering context to receiver: {:?}", error);
            }
        }
    }
}

/// A blanket implementation of `SubmitExt` for all types implementing `MeteringContext` that are
/// `Send` and `'static`.
#[async_trait::async_trait]
impl<T> SubmitExt for T where T: MeteringContext + ::std::marker::Send + 'static {}
