use chroma_error::ChromaError;
use parking_lot::Mutex;
use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    sync::Arc,
};
use thiserror::Error;

pub trait Injectable: Any + Send + Sync + Clone {}

/// A simple registry that stores any type that implements `Injectable`.
/// This is a simple implementation of a service locator pattern.
/// ## Note
/// Types stored in the registry will be cloned when retrieved.
/// Therefore, it is recommended to store types that are cheap to clone and
/// also that are "Shareable" - i.e cloning them results in non-divergent state
/// upon Mutation. (Commonly implemented via Arc<Inner> pattern)
#[derive(Default)]
pub struct Registry {
    storage: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("Type [{0}] not found in the registry")]
    TypeNotFound(String),
}

impl ChromaError for RegistryError {
    fn code(&self) -> chroma_error::ErrorCodes {
        chroma_error::ErrorCodes::Internal
    }
}

impl Registry {
    pub fn new() -> Self {
        Self {
            storage: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn register<T: Injectable>(&self, value: T) {
        let mut storage = self.storage.lock();
        storage.insert(TypeId::of::<T>(), Box::new(value));
    }

    pub fn get<T: Injectable>(&self) -> Result<T, RegistryError> {
        let storage = self.storage.lock();
        storage
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
            .cloned()
            .ok_or(RegistryError::TypeNotFound(type_name::<T>().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::AtomicUsize, Arc};

    use super::*;

    #[derive(Clone, Default)]
    struct TestInjectable {
        inner: Arc<AtomicUsize>,
    }

    impl Injectable for TestInjectable {}

    #[test]
    fn test_registry_returns_same() {
        let registry = Registry::new();
        let injectable = TestInjectable::default();
        registry.register(injectable);
        let retrieved_1 = registry
            .get::<TestInjectable>()
            .expect("To be able to get the TestInjectable");
        assert_eq!(
            retrieved_1.inner.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        retrieved_1
            .inner
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let retrieved_2 = registry
            .get::<TestInjectable>()
            .expect("To be able to get the TestInjectable");
        assert_eq!(
            retrieved_2.inner.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}
