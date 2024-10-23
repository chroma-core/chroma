use uuid::Uuid;

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) enum BlockfileWriterMutationOrdering {
    #[default]
    Unordered,
    Ordered,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) enum BlockfileWriterSplitMode {
    #[default]
    OnMutations,
    AtCommit,
}

#[derive(Debug, Default)]
pub struct BlockfileWriterOptions {
    pub(crate) mutation_ordering: BlockfileWriterMutationOrdering,
    pub(crate) split_mode: BlockfileWriterSplitMode,
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

    /// Split blocks over the block size limit on mutations (`.set()`).
    pub fn split_on_mutations(mut self) -> Self {
        self.split_mode = BlockfileWriterSplitMode::OnMutations;
        self
    }

    /// Split blocks over the block size limit at commit time.
    pub fn split_at_commit(mut self) -> Self {
        self.split_mode = BlockfileWriterSplitMode::AtCommit;
        self
    }

    /// Fork from an existing blockfile.
    pub fn fork(mut self, fork: Uuid) -> Self {
        self.fork = Some(fork);
        self
    }
}
