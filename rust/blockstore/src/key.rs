use super::Key;
use chroma_error::{ChromaError, ErrorCodes};
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

// TODO(rescrv):  This used to be a panic/unwrap, but could be a nicer type.
#[derive(thiserror::Error, Debug)]
#[error("runtime type error")]
pub struct InvalidKeyConversion;

impl ChromaError for InvalidKeyConversion {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub enum KeyWrapper {
    String(String),
    Float32(f32),
    Bool(bool),
    Uint32(u32),
}

impl KeyWrapper {
    pub(crate) fn get_size(&self) -> usize {
        match self {
            // TOOD: use key trait if possible
            KeyWrapper::String(s) => s.len(),
            KeyWrapper::Float32(_) => 4,
            KeyWrapper::Bool(_) => 1,
            KeyWrapper::Uint32(_) => 4,
        }
    }
}

impl From<&str> for KeyWrapper {
    fn from(s: &str) -> KeyWrapper {
        KeyWrapper::String(s.to_string())
    }
}

impl<'referred_data> TryFrom<&'referred_data KeyWrapper> for &'referred_data str {
    type Error = InvalidKeyConversion;

    fn try_from(key: &'referred_data KeyWrapper) -> Result<Self, InvalidKeyConversion> {
        match key {
            KeyWrapper::String(s) => Ok(s),
            _ => Err(InvalidKeyConversion),
        }
    }
}

impl From<f32> for KeyWrapper {
    fn from(f: f32) -> KeyWrapper {
        KeyWrapper::Float32(f)
    }
}

impl TryFrom<&KeyWrapper> for f32 {
    type Error = InvalidKeyConversion;

    fn try_from(key: &KeyWrapper) -> Result<Self, InvalidKeyConversion> {
        match key {
            KeyWrapper::Float32(f) => Ok(*f),
            _ => Err(InvalidKeyConversion),
        }
    }
}

impl From<bool> for KeyWrapper {
    fn from(b: bool) -> KeyWrapper {
        KeyWrapper::Bool(b)
    }
}

impl TryFrom<&KeyWrapper> for bool {
    type Error = InvalidKeyConversion;

    fn try_from(key: &KeyWrapper) -> Result<Self, InvalidKeyConversion> {
        match key {
            KeyWrapper::Bool(b) => Ok(*b),
            _ => Err(InvalidKeyConversion),
        }
    }
}

impl From<u32> for KeyWrapper {
    fn from(u: u32) -> KeyWrapper {
        KeyWrapper::Uint32(u)
    }
}

impl TryFrom<&KeyWrapper> for u32 {
    type Error = InvalidKeyConversion;

    fn try_from(key: &KeyWrapper) -> Result<Self, InvalidKeyConversion> {
        match key {
            KeyWrapper::Uint32(u) => Ok(*u),
            _ => Err(InvalidKeyConversion),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompositeKey {
    pub(super) prefix: String,
    pub(super) key: KeyWrapper,
}

impl CompositeKey {
    pub(super) fn new<K: Key>(prefix: String, key: K) -> Self {
        Self {
            prefix,
            key: key.into(),
        }
    }
}

impl Hash for CompositeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: Implement a better hash function. This is only used by the
        // memory blockfile, so its not a performance issue, since that
        // is only used for testing.
        self.prefix.hash(state)
    }
}

impl PartialEq for CompositeKey {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl Eq for CompositeKey {}

impl PartialOrd for CompositeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompositeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.prefix == other.prefix {
            match self.key {
                KeyWrapper::String(ref s1) => match &other.key {
                    KeyWrapper::String(s2) => s1.cmp(s2),
                    _ => panic!("Invalid comparison"),
                },
                KeyWrapper::Float32(f1) => match &other.key {
                    KeyWrapper::Float32(f2) => f1.partial_cmp(f2).unwrap(),
                    _ => panic!("Invalid comparison"),
                },
                KeyWrapper::Bool(b1) => match &other.key {
                    KeyWrapper::Bool(b2) => b1.cmp(b2),
                    _ => panic!("Invalid comparison"),
                },
                KeyWrapper::Uint32(u1) => match &other.key {
                    KeyWrapper::Uint32(u2) => u1.cmp(u2),
                    _ => panic!("Invalid comparison"),
                },
            }
        } else {
            self.prefix.cmp(&other.prefix)
        }
    }
}
