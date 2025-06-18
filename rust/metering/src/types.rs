use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::ops::Deref;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

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
