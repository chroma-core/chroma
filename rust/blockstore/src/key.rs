use std::hash::{Hash, Hasher};

use super::Key;

#[derive(Clone, PartialEq, PartialOrd, Debug)]
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

impl Into<KeyWrapper> for &str {
    fn into(self) -> KeyWrapper {
        KeyWrapper::String(self.to_string())
    }
}

impl<'referred_data> From<&'referred_data KeyWrapper> for &'referred_data str {
    fn from(key: &'referred_data KeyWrapper) -> Self {
        match key {
            KeyWrapper::String(s) => s,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl Into<KeyWrapper> for f32 {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Float32(self)
    }
}

impl From<&KeyWrapper> for f32 {
    fn from(key: &KeyWrapper) -> Self {
        match key {
            KeyWrapper::Float32(f) => f.clone(),
            _ => panic!("Invalid conversion"),
        }
    }
}

impl Into<KeyWrapper> for bool {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Bool(self)
    }
}

impl From<&KeyWrapper> for bool {
    fn from(key: &KeyWrapper) -> Self {
        match key {
            KeyWrapper::Bool(b) => b.clone(),
            _ => panic!("Invalid conversion"),
        }
    }
}

impl Into<KeyWrapper> for u32 {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Uint32(self)
    }
}

impl From<&KeyWrapper> for u32 {
    fn from(key: &KeyWrapper) -> Self {
        match key {
            KeyWrapper::Uint32(u) => u.clone(),
            _ => panic!("Invalid conversion"),
        }
    }
}

#[derive(Clone, Debug)]
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
        // TODO: Implement a better hash function
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
        if self.prefix == other.prefix {
            self.key.partial_cmp(&other.key)
        } else {
            self.prefix.partial_cmp(&other.prefix)
        }
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
