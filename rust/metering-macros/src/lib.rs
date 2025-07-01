extern crate proc_macro;

use std::collections::HashMap;

use proc_macro2::{Ident, TokenStream};
use quote::quote;

use crate::{
    capabilities::{generate_capability_marker_method_definition, Capability},
    contexts::{
        generate_capability_implementation_for_base_context,
        generate_capability_implementations_for_context,
    },
    parsing::process_token_stream,
};

mod capabilities;
mod contexts;
mod errors;
mod parsing;

/// The single user-facing macro export of this crate that outputs the metering library
/// source code, as well as the necessary trait implementations to allow capabilities
/// to be used on contexts.
#[proc_macro]
pub fn initialize_metering(raw_token_stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let token_stream = TokenStream::from(raw_token_stream);

    let (capabilities, contexts, passthroughs) = match process_token_stream(&token_stream) {
        Ok(result) => result,
        Err(error) => return proc_macro::TokenStream::from(error.to_compile_error()),
    };

    let capability_names_to_capabilities: HashMap<Ident, Capability> = capabilities
        .iter()
        .map(|capability| (capability.capability_name_ident.clone(), capability.clone()))
        .collect();

    let capability_marker_method_definitions: Vec<TokenStream> = capabilities
        .iter()
        .map(generate_capability_marker_method_definition)
        .collect();

    let capability_implementations_for_contexts: Vec<TokenStream> = contexts
        .iter()
        .map(|context| {
            generate_capability_implementations_for_context(
                context,
                &capability_names_to_capabilities,
            )
        })
        .collect();

    let capability_implementations_for_base_context: Vec<TokenStream> = capabilities
        .iter()
        .map(generate_capability_implementation_for_base_context)
        .collect();

    proc_macro::TokenStream::from(quote! {

        /// Allow unrecognized code to pass through.
        #( #passthroughs )*

        /// The base trait which all contexts implement.
        pub trait MeteringContext: ::std::fmt::Debug + ::std::any::Any + ::std::marker::Send + ::std::marker::Sync + 'static {
            fn as_any(&self) -> &dyn ::std::any::Any;

            #( #capability_marker_method_definitions )*
        }

        /// Implementations of capabilities for each context.
        #( #capability_implementations_for_contexts )*

        /// Implementations of capabilities for the base trait object `dyn MeteringContext`.
        #( #capability_implementations_for_base_context )*

        #[derive(::std::fmt::Debug, Clone)]
        pub struct BlankMeteringContext;

        impl MeteringContext for BlankMeteringContext {
            fn as_any(&self) -> &dyn ::std::any::Any {
                self
            }
        }

        /// A runtime error that occurs in the metering library
        #[derive(Debug, thiserror::Error)]
        pub enum MeteringError {
            #[error("The metering context receiver has already been initialized")]
            ReceiverAlreadyInitializedError,
            #[error("Failed to downcast context to provided type")]
            DowncastError,
            #[error("RwLock poisoned when attempting to read or write")]
            RwLockPoisonedError,
        }

        /// A type alias for a shared, boxed, metering context
        pub type MeteringContextContainer = ::std::sync::Arc<dyn MeteringContext>;

        /// Allows `MeteringContextContainer` to be entered and exited for synchronous programs
        pub trait Enterable {
            fn enter(&self);

            fn exit(&self);
        }

        impl Enterable for MeteringContextContainer {
            /// Enter sets the current thread's context to this context
            fn enter(&self) {
                ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
                    cell.replace(self.clone());
                });
            }

            /// Exit clears the current thread's active metering context
            fn exit(&self) {
                ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
                    cell.replace(::std::sync::Arc::new(BlankMeteringContext));
                });
            }
        }

        // Thread-local storage for the active metering context
        ::std::thread_local! {
            static ACTIVE_METERING_CONTEXT_CONTAINER: ::std::cell::RefCell<MeteringContextContainer> =
                ::std::cell::RefCell::new(::std::sync::Arc::new(BlankMeteringContext));
        }

        /// Creates a metering context of type `C` and returns a `MeteringContextContainer`
        pub fn create<C: MeteringContext>(metering_context: C) -> MeteringContextContainer {
            let metering_context_container = ::std::sync::Arc::new(metering_context);
            metering_context_container
        }

        /// Allows users to specify a closure to invoke on the current thread's active metering context.
        /// If no context is active, this will be a no-op because the mutation will be applied to
        /// `BlankMeteringContext`
        pub fn with_current(mutator: impl FnOnce(&dyn MeteringContext)) {
            ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
                let active_metering_context_container = cell.borrow();
                mutator(&**active_metering_context_container as &dyn MeteringContext);
            })
        }

        /// Gets the current metering context and returns it as `MeteringContextContainer`,
        /// incrementing its reference count
        pub fn get_current() -> MeteringContextContainer {
            ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
                let active_metering_context_container = cell.borrow();
                return active_metering_context_container.clone();
            })
        }

        /// Closes the current thread's metering context if it is of type `C`, otherwise returns an error
        pub fn close<C: MeteringContext + Clone>() -> Result<C, MeteringError> {
            ACTIVE_METERING_CONTEXT_CONTAINER.with(|cell| {
                let mut active_metering_context_container = cell.borrow_mut();

                let metering_context = (**active_metering_context_container)
                    .as_any()
                    .downcast_ref::<C>()
                    .map(Clone::clone)
                    .ok_or(MeteringError::DowncastError)?;

                *active_metering_context_container = ::std::sync::Arc::new(BlankMeteringContext);

                Ok(metering_context)
            })
        }

        /// A trait that allows futures to be metered, similar to how `tracing` enables futures to be
        /// instrumented
        pub trait MeteredFutureExt: ::std::future::Future + Sized {
            fn meter(self, metering_context_container: MeteringContextContainer) -> MeteredFuture<Self> {
                MeteredFuture {
                    inner_future: self,
                    metering_context_container,
                }
            }
        }

        /// A blanket implementation of `metered` for all futures
        impl<F: ::std::future::Future> MeteredFutureExt for F {}

        /// Similar to `tracing::Instrumented`, this wraps a future and stores the
        /// active metering context in the thread's local storage
        #[::pin_project::pin_project]
        pub struct MeteredFuture<F: ::std::future::Future> {
            #[pin]
            inner_future: F,
            metering_context_container: MeteringContextContainer,
        }

        /// Handles setting the current thread's active metering context when it is polled and
        /// unsetting it after the poll is complete
        impl<F: ::std::future::Future> ::std::future::Future for MeteredFuture<F> {
            type Output = F::Output;

            fn poll(
                self: ::std::pin::Pin<&mut Self>,
                context: &mut ::std::task::Context<'_>,
            ) -> ::std::task::Poll<Self::Output> {
                let this = self.project();

                this.metering_context_container.enter();

                let output = this.inner_future.poll(context);

                this.metering_context_container.exit();

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
        #[::async_trait::async_trait]
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
        #[::async_trait::async_trait]
        impl<T> SubmitExt for T where T: MeteringContext + ::std::marker::Send + 'static {}
    })
}
