use arrow::util::bit_util;
use std::sync::{atomic::AtomicUsize, Arc};

/// A simple size tracker for use internally in Arrow Deltas that
/// only have a single column value. More complex value types (such as
/// DataRecord) should use their own size tracking rather than this one.
/// Ideally we would have a more generic size tracker that could be used
/// for any type of value - but this is a quick start :)
#[derive(Clone, Debug)]
pub(super) struct SingleValueSizeTracker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    prefix_size: AtomicUsize,
    key_size: AtomicUsize,
    value_size: AtomicUsize,
}

impl SingleValueSizeTracker {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                prefix_size: AtomicUsize::new(0),
                key_size: AtomicUsize::new(0),
                value_size: AtomicUsize::new(0),
            }),
        }
    }

    pub(super) fn with_values(prefix_size: usize, key_size: usize, value_size: usize) -> Self {
        Self {
            inner: Arc::new(Inner {
                prefix_size: AtomicUsize::new(prefix_size),
                key_size: AtomicUsize::new(key_size),
                value_size: AtomicUsize::new(value_size),
            }),
        }
    }

    /// The raw unpadded size of the prefix data in bytes.
    pub(super) fn get_prefix_size(&self) -> usize {
        self.inner
            .prefix_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The arrow padded size of the prefix data in bytes.
    pub(super) fn get_arrow_padded_prefix_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_prefix_size())
    }

    /// The raw unpadded size of the key data in bytes.
    pub(super) fn get_key_size(&self) -> usize {
        self.inner
            .key_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The arrow padded size of the key data in bytes.
    /// This does not include potential other bytes used by arrow such as
    /// the validity bitmap and offsets.
    pub(super) fn get_arrow_padded_key_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_key_size())
    }

    /// The raw unpadded size of the value data in bytes.
    pub(super) fn get_value_size(&self) -> usize {
        self.inner
            .value_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The arrow padded size of the value data in bytes.
    /// This does not include potential other bytes used by arrow such as
    /// the validity bitmap and offsets.
    pub(super) fn get_arrow_padded_value_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_value_size())
    }

    pub(super) fn add_prefix_size(&self, size: usize) {
        self.inner
            .prefix_size
            .fetch_add(size, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn add_key_size(&self, size: usize) {
        self.inner
            .key_size
            .fetch_add(size, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn add_value_size(&self, size: usize) {
        self.inner
            .value_size
            .fetch_add(size, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn subtract_prefix_size(&self, size: usize) {
        self.inner
            .prefix_size
            .fetch_sub(size, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn subtract_key_size(&self, size: usize) {
        self.inner
            .key_size
            .fetch_sub(size, std::sync::atomic::Ordering::SeqCst);
    }

    pub(super) fn subtract_value_size(&self, size: usize) {
        self.inner
            .value_size
            .fetch_sub(size, std::sync::atomic::Ordering::SeqCst);
    }
}
