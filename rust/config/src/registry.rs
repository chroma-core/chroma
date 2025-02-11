use parking_lot::Mutex;
use std::{
    any::{Any, TypeId},
    collections::HashMap,
};

pub trait Injectable: Any + Send + Sync + Clone {}

/// A simple registry that stores any type that implements `Injectable`.
/// This is a simple implementation of a service locator pattern.
/// ## Note
/// Types stored in the registry will be cloned when retrieved.
/// Therefore, it is recommended to store types that are cheap to clone and
/// also that are "Shareable" - i.e cloning them results in non-divergent state
/// upon Mutation. (Commonly implemented via Arc<Inner> pattern)
struct Registry {
    storage: Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync>>>,
}

impl Registry {
    fn new() -> Self {
        Self {
            storage: Mutex::new(HashMap::new()),
        }
    }

    fn insert<T: Injectable>(&self, value: Box<T>) {
        let mut storage = self.storage.lock();
        storage.insert(TypeId::of::<T>(), value);
    }

    fn get<T: Injectable>(&self) -> Option<T> {
        let storage = self.storage.lock();
        storage
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref::<T>())
            .cloned()
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
        let injectable = Box::new(TestInjectable::default());
        registry.insert(injectable);
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
