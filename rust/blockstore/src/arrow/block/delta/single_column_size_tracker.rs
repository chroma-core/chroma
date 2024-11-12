use arrow::util::bit_util;

/// A simple size tracker for use internally in Arrow Deltas that
/// only have a single column value. More complex value types (such as
/// DataRecord) should use their own size tracking rather than this one.
/// Ideally we would have a more generic size tracker that could be used
/// for any type of value - but this is a quick start :)
/// ## Note
/// This struct is not thread safe and users are expected to handle
/// synchronization themselves.
#[derive(Clone, Debug)]
pub(super) struct SingleColumnSizeTracker {
    prefix_size: usize,
    key_size: usize,
    value_size: usize,
}

impl SingleColumnSizeTracker {
    pub(super) fn new() -> Self {
        Self {
            prefix_size: 0,
            key_size: 0,
            value_size: 0,
        }
    }

    pub(super) fn with_values(prefix_size: usize, key_size: usize, value_size: usize) -> Self {
        Self {
            prefix_size,
            key_size,
            value_size,
        }
    }

    /// The raw unpadded size of the prefix data in bytes.
    pub(super) fn get_prefix_size(&self) -> usize {
        self.prefix_size
    }

    /// The arrow padded size of the prefix data in bytes.
    pub(super) fn get_arrow_padded_prefix_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_prefix_size())
    }

    /// The raw unpadded size of the key data in bytes.
    pub(super) fn get_key_size(&self) -> usize {
        self.key_size
    }

    /// The arrow padded size of the key data in bytes.
    /// This does not include potential other bytes used by arrow such as
    /// the validity bitmap and offsets.
    pub(super) fn get_arrow_padded_key_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_key_size())
    }

    /// The raw unpadded size of the value data in bytes.
    pub(super) fn get_value_size(&self) -> usize {
        self.value_size
    }

    /// The arrow padded size of the value data in bytes.
    /// This does not include potential other bytes used by arrow such as
    /// the validity bitmap and offsets.
    pub(super) fn get_arrow_padded_value_size(&self) -> usize {
        bit_util::round_upto_multiple_of_64(self.get_value_size())
    }

    pub(super) fn add_prefix_size(&mut self, size: usize) {
        self.prefix_size += size;
    }

    pub(super) fn add_key_size(&mut self, size: usize) {
        self.key_size += size;
    }

    pub(super) fn add_value_size(&mut self, size: usize) {
        self.value_size += size;
    }

    pub(super) fn subtract_prefix_size(&mut self, size: usize) {
        self.prefix_size -= size;
    }

    pub(super) fn subtract_key_size(&mut self, size: usize) {
        self.key_size -= size;
    }

    pub(super) fn subtract_value_size(&mut self, size: usize) {
        self.value_size -= size;
    }
}
