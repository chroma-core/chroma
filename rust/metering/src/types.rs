use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};

/// A wrapper around `Mutex<T>` that implements `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`,
/// assuming `T` implements those traits.
pub struct MeteringMutex<T>(pub Mutex<T>);

impl<T: Clone> Clone for MeteringMutex<T> {
    fn clone(&self) -> Self {
        MeteringMutex(Mutex::new(self.0.lock().unwrap().clone()))
    }
}

impl<T: PartialEq> PartialEq for MeteringMutex<T> {
    fn eq(&self, other: &Self) -> bool {
        *self.0.lock().unwrap() == *other.0.lock().unwrap()
    }
}

impl<T: Eq> Eq for MeteringMutex<T> {}

impl<T> Deref for MeteringMutex<T> {
    type Target = Mutex<T>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for MeteringMutex<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: fmt::Debug> fmt::Debug for MeteringMutex<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.lock().unwrap().fmt(formatter)
    }
}
impl<T: Serialize> Serialize for MeteringMutex<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_mutex::serialize(&self.0, serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for MeteringMutex<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_mutex::deserialize(deserializer).map(MeteringMutex)
    }
}

/// Internal module to support serde for `Mutex<T>`
mod serde_mutex {
    use super::*;
    pub fn serialize<T: Serialize, S: Serializer>(
        mutex: &Mutex<T>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        mutex.lock().unwrap().serialize(serializer)
    }

    pub fn deserialize<'de, T: Deserialize<'de>, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Mutex<T>, D::Error> {
        let inner = T::deserialize(deserializer)?;
        Ok(Mutex::new(inner))
    }
}

/// A wrapper around `Arc<AtomicU64>` that implements `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`.
#[derive(Clone)]
pub struct MeteringAtomicU64(pub Arc<AtomicU64>);

impl PartialEq for MeteringAtomicU64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.load(Ordering::SeqCst) == other.0.load(Ordering::SeqCst)
    }
}

impl Eq for MeteringAtomicU64 {}

impl Deref for MeteringAtomicU64 {
    type Target = AtomicU64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for MeteringAtomicU64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("MeteringAtomicU64")
            .field(&self.0.load(Ordering::SeqCst))
            .finish()
    }
}

impl Serialize for MeteringAtomicU64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0.load(Ordering::SeqCst))
    }
}

impl<'de> Deserialize<'de> for MeteringAtomicU64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = u64::deserialize(deserializer)?;
        Ok(MeteringAtomicU64(Arc::new(AtomicU64::new(value))))
    }
}
