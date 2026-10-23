use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::sync::RwLock;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Instant;

use crate::core::MeteringError;

/// A wrapper around `Arc<AtomicU64>` that implements `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, and `Deserialize`.
#[derive(Clone)]
pub struct MeteringAtomicU64(pub Arc<AtomicU64>);

impl MeteringAtomicU64 {
    pub fn new(value: u64) -> Self {
        MeteringAtomicU64(Arc::new(AtomicU64::new(value)))
    }
}

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

/// A wrapper around `Arc<RwLock<Instant>>` that implements `Clone` and `Debug`.
#[derive(Clone)]
pub struct MeteringInstant(Arc<RwLock<Instant>>);

impl MeteringInstant {
    pub fn now() -> Self {
        Self(Arc::new(RwLock::new(Instant::now())))
    }

    pub fn load(&self) -> Result<Instant, MeteringError> {
        let guard = self
            .0
            .read()
            .map_err(|_| MeteringError::RwLockPoisonedError)?;
        Ok(*guard)
    }

    pub fn store(&self, instant: Instant) -> Result<(), MeteringError> {
        let mut guard = self
            .0
            .write()
            .map_err(|_| MeteringError::RwLockPoisonedError)?;
        *guard = instant;
        Ok(())
    }
}

impl fmt::Debug for MeteringInstant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.read() {
            Ok(guard) => f.debug_tuple("MeteringInstant").field(&*guard).finish(),
            Err(_) => f
                .debug_tuple("MeteringInstant")
                .field(&"<poisoned>")
                .finish(),
        }
    }
}

impl PartialEq for MeteringInstant {
    fn eq(&self, other: &Self) -> bool {
        let Ok(self_instant) = self.0.read() else {
            return false;
        };
        let Ok(other_instant) = other.0.read() else {
            return false;
        };
        *self_instant == *other_instant
    }
}
