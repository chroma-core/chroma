use uuid::Uuid;

#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) enum BlockfileWriterMutationOrdering {
    #[default]
    Unordered,
    Ordered,
}

#[derive(Debug, Default, Clone)]
pub struct BlockfileWriterOptions {
    pub(crate) mutation_ordering: BlockfileWriterMutationOrdering,
    pub(crate) fork: Option<Uuid>,
}

impl BlockfileWriterOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// No guarantees are made about the order of mutations (calls to `.set()` and `.delete()`).
    pub fn unordered_mutations(mut self) -> Self {
        self.mutation_ordering = BlockfileWriterMutationOrdering::Unordered;
        self
    }

    /// Mutations (calls to `.set()` and `.delete()`) are provided in ascending order of keys. This mode should be preferred when possible as it's more efficient than unordered mutations. Blockfile implementations may panic when in this mode if:
    /// - mutations are not provided in sequential order
    /// - a key is provided more than once (e.g. a key is provided to both `.set()` and `.delete()`)
    pub fn ordered_mutations(mut self) -> Self {
        self.mutation_ordering = BlockfileWriterMutationOrdering::Ordered;
        self
    }

    /// Fork from an existing blockfile.
    pub fn fork(mut self, fork: Uuid) -> Self {
        self.fork = Some(fork);
        self
    }
}
