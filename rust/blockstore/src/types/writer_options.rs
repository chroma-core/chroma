use uuid::Uuid;

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub enum BlockfileWriterMutationOrdering {
    #[default]
    Unordered,
    Ordered,
}

#[derive(Debug, Clone)]
pub struct BlockfileWriterOptions {
    pub(crate) mutation_ordering: BlockfileWriterMutationOrdering,
    pub(crate) fork_from: Option<Uuid>,
    pub(crate) prefix_path: String,
    pub(crate) max_block_size_bytes: Option<usize>,
}

impl BlockfileWriterOptions {
    pub fn new(prefix_path: String) -> Self {
        BlockfileWriterOptions {
            prefix_path,
            fork_from: None,
            mutation_ordering: BlockfileWriterMutationOrdering::default(),
            max_block_size_bytes: None,
        }
    }

    /// No guarantees are made about the order of mutations (calls to `.set()` and `.delete()`).
    pub fn unordered_mutations(mut self) -> Self {
        self.mutation_ordering = BlockfileWriterMutationOrdering::Unordered;
        self
    }

    /// Mutations (calls to `.set()` and `.delete()`) are provided in ascending order of keys. This mode may be more efficient if your data is pre-sorted. Blockfile implementations may return an error when in this mode if:
    /// - mutations are not provided in sequential order
    /// - a key is provided more than once (e.g. a key is provided to both `.set()` and `.delete()`)
    pub fn ordered_mutations(mut self) -> Self {
        self.mutation_ordering = BlockfileWriterMutationOrdering::Ordered;
        self
    }

    pub fn set_mutation_ordering(mut self, ordering: BlockfileWriterMutationOrdering) -> Self {
        self.mutation_ordering = ordering;
        self
    }

    /// Fork from an existing blockfile.
    pub fn fork(mut self, fork: Uuid) -> Self {
        self.fork_from = Some(fork);
        self
    }

    pub fn max_block_size_bytes(mut self, size: usize) -> Self {
        self.max_block_size_bytes = Some(size);
        self
    }
}
