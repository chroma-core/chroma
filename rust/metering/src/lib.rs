extern crate proc_macro;

use proc_macro2::TokenStream;
use quote::quote;

use crate::{
    attributes::generate_attribute_definition_token_stream,
    contexts::generate_context_definition_token_stream,
    mutators::generate_noop_mutator_definition_token_stream,
    utils::{generate_compile_error, process_token_stream},
};

mod annotations;
mod attributes;
mod contexts;
mod errors;
mod fields;
mod mutators;
mod utils;

/// This is the only user-facing export of `chroma_metering`. It is responsible for registering attributes and
/// contexts by producing the code necessary to allow users to interact with the metering library.
#[proc_macro]
pub fn initialize_metering(raw_token_stream: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let token_stream = TokenStream::from(raw_token_stream);

    let (attributes, contexts) = match process_token_stream(&token_stream) {
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

    let context_definition_token_streams: Vec<TokenStream> = contexts
        .iter()
        .map(generate_context_definition_token_stream)
        .collect();

    proc_macro::TokenStream::from(quote! {
        /// The base trait that is programmatically implemented for all user-defined metering
        /// contexts
        pub trait MeteringContext:
            ::std::fmt::Debug + ::std::any::Any + ::std::marker::Send + 'static
        {
            #( #noop_mutator_definition_token_streams )*

            fn clone_box(&self) -> Box<dyn MeteringContext>;

            fn as_any(&self) -> &dyn ::std::any::Any;
        }

        /// An implementation of `Clone` for boxed trait objects of `MeteringContext`
        impl Clone for Box<dyn MeteringContext> {
            fn clone(&self) -> Box<dyn MeteringContext> {
                self.clone_box()
            }
        }

        #( #attribute_definition_token_streams )*
        #( #context_definition_token_streams )*

        /// A blank metering context to use when there is no active metering context
        #[derive(::std::fmt::Debug, Clone)]
        struct BlankMeteringContext;

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
            #[error("Requested type is not the same as the active context's type on this thread")]
            TypeMismatchError,
            #[error(
                "Failed to downcast context from std::any::Any to provided type, despite type IDs matching"
            )]
            DowncastError,
        }

        /// A container struct for the active metering context that also stores its TypeId
        struct MeteringContextContainer {
            boxed_metering_context: Box<dyn MeteringContext>,
            metering_context_type_id: std::any::TypeId,
        }

        /// Default values for the `MeteringContextContainer`
        impl std::default::Default for MeteringContextContainer {
            fn default() -> Self {
                MeteringContextContainer {
                    boxed_metering_context: Box::new(BlankMeteringContext),
                    metering_context_type_id: std::any::TypeId::of::<BlankMeteringContext>(),
                }
            }
        }

        /// Thread-local storage of the active metering context
        ::std::thread_local! {
            static ACTIVE_METERING_CONTEXT_CONTAINER: ::std::cell::RefCell<MeteringContextContainer> =
                ::std::cell::RefCell::new(MeteringContextContainer::default());
        }

        /// Creates a metering context of type `C` and returns a guard for RAII
        pub fn create<C: MeteringContext>(metering_context: C) -> MeteringContextGuard {
            let metering_context_type_id = ::std::any::TypeId::of::<C>();
            let boxed_metering_context: Box<dyn MeteringContext> = Box::new(metering_context);

            ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                active_metering_context_container.replace(MeteringContextContainer {
                    boxed_metering_context: boxed_metering_context.clone(),
                    metering_context_type_id,
                });
            });

            MeteringContextGuard {
                inner_boxed_metering_context: boxed_metering_context,
                inner_metering_context_type_id: metering_context_type_id,
            }
        }

        /// Allows users to specify a closure to invoke on the current thread's active metering context.
        /// If no context is active, this will be a no-op because the mutation will be applied to
        /// `BlankMeteringContext`
        pub fn with_current(mutator: impl FnOnce(&mut dyn MeteringContext)) {
            ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                let mut mutable_active_metering_context_container = active_metering_context_container.borrow_mut();
                let active_metering_context: &mut dyn MeteringContext = mutable_active_metering_context_container
                    .boxed_metering_context
                    .as_mut();
                mutator(active_metering_context);
            });
        }

        /// Closes the current thread's metering context if it is of type `C`, otherwise returns an error
        pub fn close<C: MeteringContext + Clone>() -> Result<C, MeteringError> {
            let metering_context_type_id = std::any::TypeId::of::<C>();

            ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                let mut mutable_active_metering_context_container = active_metering_context_container.borrow_mut();

                let active_metering_context = std::mem::replace(
                    &mut mutable_active_metering_context_container.boxed_metering_context,
                    Box::new(BlankMeteringContext),
                );
                let active_metering_context_type_id = std::mem::replace(
                    &mut mutable_active_metering_context_container.metering_context_type_id,
                    std::any::TypeId::of::<BlankMeteringContext>(),
                );

                if active_metering_context_type_id == metering_context_type_id {
                    match active_metering_context.as_any().downcast_ref::<C>() {
                        Some(metering_context) => Ok(metering_context.clone()),
                        None => Err(MeteringError::DowncastError),
                    }
                } else {
                    mutable_active_metering_context_container.boxed_metering_context = active_metering_context;
                    mutable_active_metering_context_container.metering_context_type_id = active_metering_context_type_id;
                    Err(MeteringError::TypeMismatchError)
                }
            })
        }

        /// A guard for RAII that stores a metering context and its type ID
        pub struct MeteringContextGuard {
            inner_boxed_metering_context: Box<dyn MeteringContext>,
            inner_metering_context_type_id: ::std::any::TypeId,
        }

        /// We implement drop for the guard to get RAII
        impl Drop for MeteringContextGuard {
            fn drop(&mut self) {
                ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                    active_metering_context_container.replace(MeteringContextContainer::default());
                });
            }
        }

        /// A trait that allows futures to be `metered`, similar to how `tracing` enables futures to be
        /// `instrumented`
        pub trait MeteredFutureExt: ::std::future::Future + Sized {
            fn metered(self, metering_context_guard: MeteringContextGuard) -> MeteredFuture<Self> {
                MeteredFuture {
                    inner_future: self,
                    metering_context_guard,
                }
            }
        }

        /// A blanket implementation of `metered` for all futures
        impl<F: ::std::future::Future> MeteredFutureExt for F {}

        /// Similar to `tracing::Instrumented`, this wraps a future and stores the
        /// active metering context in the thread's local storage
        #[pin_project::pin_project]
        pub struct MeteredFuture<F: ::std::future::Future> {
            #[pin]
            inner_future: F,
            metering_context_guard: MeteringContextGuard,
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

                ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                    active_metering_context_container.replace(MeteringContextContainer {
                        boxed_metering_context: metered_future
                            .metering_context_guard
                            .inner_boxed_metering_context
                            .clone(),
                        metering_context_type_id: metered_future
                            .metering_context_guard
                            .inner_metering_context_type_id,
                    });
                });

                let output = metered_future.inner_future.poll(context);

                // ACTIVE_METERING_CONTEXT_CONTAINER.with(|active_metering_context_container| {
                //     active_metering_context_container.replace(MeteringContextContainer::default());
                // });

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
        impl<T> SubmitExt for T
        where
            T: MeteringContext + ::std::marker::Send + 'static,
        {}
    })
}
