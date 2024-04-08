use std::hash::{Hash, Hasher};

#[derive(Clone, PartialEq)]
pub(crate) enum KeyWrapper {
    String(String),
    Float32(f32),
    Bool(bool),
    Uint(u32),
}

impl Into<KeyWrapper> for String {
    fn into(self) -> KeyWrapper {
        KeyWrapper::String(self)
    }
}

impl Into<KeyWrapper> for f32 {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Float32(self)
    }
}

impl Into<KeyWrapper> for bool {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Bool(self)
    }
}

impl Into<KeyWrapper> for u32 {
    fn into(self) -> KeyWrapper {
        KeyWrapper::Uint(self)
    }
}

pub(super) struct StoredBlockfileKey {
    pub(super) prefix: String,
    pub(super) key: KeyWrapper,
}

impl Hash for StoredBlockfileKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: Implement a better hash function
        self.prefix.hash(state)
    }
}

impl PartialEq for StoredBlockfileKey {
    fn eq(&self, other: &Self) -> bool {
        self.prefix == other.prefix && self.key == other.key
    }
}

impl Eq for StoredBlockfileKey {}
