/// This test uses the proptest-state-machine crate to generate a sequence of transitions for a blockfile writer and compares the result after every commit with a reference implementation.

#[cfg(test)]
mod tests {
    use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
    use chroma_blockstore::{
        BlockfileReader, BlockfileWriter, BlockfileWriterMutationOrdering, BlockfileWriterOptions,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::local::LocalStorage;
    use chroma_storage::Storage;
    use futures::executor::block_on;
    use itertools::Itertools;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use proptest_state_machine::prop_state_machine;
    use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
    use std::collections::{BTreeMap, BTreeSet};
    use uuid::Uuid;

    /// Possible transitions for our state machine (maps to `.set()`, `.delete()`, and `.commit()` on the blockfile writer).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum Transition {
        Set(String, String, Vec<u32>),
        Delete(String, String),
        Commit,
    }

    impl Transition {
        fn get_prefix_and_key(&self) -> Option<(&String, &String)> {
            match self {
                Transition::Set(prefix, key, _) => Some((prefix, key)),
                Transition::Delete(prefix, key) => Some((prefix, key)),
                Transition::Commit => None,
            }
        }
    }

    /// This is the reference implementation of the blockfile writer that we compare against.
    #[derive(Clone, Debug)]
    pub struct RefState {
        /// This field is not used in the reference impl, but gives a block size to the real blockfile impl
        generated_max_block_size_bytes: usize,
        /// This field is not used in the reference impl, but gives a mutation ordering to the real blockfile impl
        generated_mutation_ordering: BlockfileWriterMutationOrdering,
        dirty_keys: BTreeSet<(String, String)>,
        store: BTreeMap<(String, String), Vec<u32>>,
        last_commit: Option<BTreeMap<(String, String), Vec<u32>>>,
    }

    pub struct BlockfileWriterStateMachine {}

    impl ReferenceStateMachine for BlockfileWriterStateMachine {
        type State = RefState;
        type Transition = Transition;

        fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
            let mutation_ordering = prop_oneof![
                Just(BlockfileWriterMutationOrdering::Unordered),
                Just(BlockfileWriterMutationOrdering::Ordered)
            ];
            (1_000..2_000usize, mutation_ordering) // The block size is somewhat arbitrary; the min needs to be more than the largest possible block (after padding) containing a single entry. But it should be small enough that block splitting is likely to occur.
                .prop_map(|(block_size_bytes, generated_mutation_ordering)| RefState {
                    generated_max_block_size_bytes: block_size_bytes,
                    generated_mutation_ordering,
                    store: BTreeMap::new(),
                    dirty_keys: BTreeSet::new(),
                    last_commit: None,
                })
                .boxed()
        }

        fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
            let prefix_strategy = "[0-9a-zA-Z]{1,10}";
            let key_strategy = "[0-9a-zA-Z]{1,30}";
            let value_strategy = proptest::collection::vec(0..u32::MAX, 1..100);

            let new_set_transition = (prefix_strategy, key_strategy, value_strategy.clone())
                .prop_map(|(prefix, key, value)| Transition::Set(prefix, key, value));

            let set_transition = if state.store.is_empty() {
                new_set_transition.boxed()
            } else {
                let existing_prefix_key_set_transition = (
                    proptest::sample::select(state.store.keys().cloned().collect_vec()),
                    value_strategy.clone(),
                )
                    .prop_map(|((prefix, key), value)| Transition::Set(prefix, key, value));

                let existing_prefix_set_transition = (
                    proptest::sample::select(
                        state
                            .store
                            .keys()
                            .map(|(prefix, _)| prefix.clone())
                            .collect_vec(),
                    ),
                    key_strategy,
                    value_strategy.clone(),
                )
                    .prop_map(|(prefix, key, value)| Transition::Set(prefix, key, value));

                prop_oneof![
                    // 75% chance of setting a new key
                    6 => new_set_transition,
                    // 15% chance of setting on an existing key
                    1 => existing_prefix_set_transition,
                    // 10% chance of setting on an existing prefix and key
                    1 => existing_prefix_key_set_transition
                ]
                .boxed()
            };

            let delete_nonexisting_transition = (prefix_strategy, key_strategy)
                .prop_map(|(prefix, key)| Transition::Delete(prefix, key));

            let delete_transition = if state.store.is_empty() {
                delete_nonexisting_transition.boxed()
            } else {
                let delete_existing_transition =
                    proptest::sample::select(state.store.keys().cloned().collect_vec())
                        .prop_map(|(prefix, key)| Transition::Delete(prefix, key));

                prop_oneof![
                    9 => delete_existing_transition,
                    1 => delete_nonexisting_transition
                ]
                .boxed()
            };

            let commit_transition = Just(Transition::Commit);

            prop_oneof![
                // 80% chance of a set transition
                8 => set_transition,
                // 10% chance of a delete transition
                1 => delete_transition,
                // 10% chance of a commit transition
                1 => commit_transition
            ]
            .boxed()
        }

        fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
            if state.generated_mutation_ordering == BlockfileWriterMutationOrdering::Ordered {
                if let Some((prefix, key)) = transition.get_prefix_and_key() {
                    // When in ordered writing mode, every key/prefix pair provided to `.set()` or `.delete()` must be strictly greater than the last pair provided.
                    if let Some((max_prefix, max_key)) = state.dirty_keys.last() {
                        match max_prefix.cmp(prefix) {
                            std::cmp::Ordering::Less => return true,
                            std::cmp::Ordering::Equal => {
                                return max_key.cmp(key) == std::cmp::Ordering::Less;
                            }
                            std::cmp::Ordering::Greater => return false,
                        }
                    }
                }
            }

            true
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                Transition::Set(prefix, key, value) => {
                    state
                        .store
                        .insert((prefix.clone(), key.clone()), value.clone());
                    state.dirty_keys.insert((prefix.clone(), key.clone()));
                }
                Transition::Delete(prefix, key) => {
                    state.store.remove(&(prefix.clone(), key.clone()));
                    state.dirty_keys.insert((prefix.clone(), key.clone()));
                }
                Transition::Commit => {
                    state.last_commit = Some(state.store.clone());
                    state.dirty_keys.clear();
                }
            }

            state
        }
    }

    /// The "real" blockfile writer implementation that we compare against the reference.
    struct BlockfileWriterWrapper {
        provider: ArrowBlockfileProvider,
        last_blockfile_id: Option<Uuid>,
        writer: BlockfileWriter,
    }

    impl StateMachineTest for BlockfileWriterWrapper {
        type SystemUnderTest = Self;

        type Reference = BlockfileWriterStateMachine;

        fn init_test(
            ref_state: &<Self::Reference as proptest_state_machine::ReferenceStateMachine>::State,
        ) -> Self::SystemUnderTest {
            let storage_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(storage_dir.path().to_str().unwrap()));
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let provider = ArrowBlockfileProvider::new(
                storage,
                ref_state.generated_max_block_size_bytes,
                block_cache,
                sparse_index_cache,
            );
            let writer = block_on(
                provider.write::<&str, Vec<u32>>(
                    BlockfileWriterOptions::new()
                        .set_mutation_ordering(ref_state.generated_mutation_ordering),
                ),
            )
            .unwrap();

            BlockfileWriterWrapper {
                provider,
                last_blockfile_id: None,
                writer,
            }
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            ref_state: &<Self::Reference as proptest_state_machine::ReferenceStateMachine>::State,
            transition: <Self::Reference as proptest_state_machine::ReferenceStateMachine>::Transition,
        ) -> Self::SystemUnderTest {
            match transition {
                Transition::Set(prefix, key, value) => {
                    block_on(state.writer.set(prefix.as_str(), key.as_str(), value)).unwrap();
                }
                Transition::Delete(prefix, key) => {
                    block_on(
                        state
                            .writer
                            .delete::<&str, Vec<u32>>(prefix.as_str(), key.as_str()),
                    )
                    .unwrap();
                }
                Transition::Commit => {
                    let id = state.writer.id();
                    let flusher = block_on(state.writer.commit::<&str, Vec<u32>>()).unwrap();
                    block_on(flusher.flush::<&str, Vec<u32>>()).unwrap();

                    state.last_blockfile_id = Some(id);
                    state.writer = block_on(
                        state.provider.write::<&str, Vec<u32>>(
                            BlockfileWriterOptions::new()
                                .set_mutation_ordering(ref_state.generated_mutation_ordering)
                                .fork(id),
                        ),
                    )
                    .unwrap();
                }
            }

            state
        }

        fn check_invariants(
            state: &Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) {
            if ref_state.last_commit.is_none() || state.last_blockfile_id.is_none() {
                return;
            }

            let ref_last_commit = ref_state.last_commit.as_ref().unwrap();
            let last_blockfile_id = state.last_blockfile_id.unwrap();

            let reader = block_on(state.provider.read::<&str, &[u32]>(&last_blockfile_id)).unwrap();

            // Check count
            assert_eq!(block_on(reader.count()).unwrap(), ref_last_commit.len());

            // Check that entries are ordered and match expected
            if !ref_last_commit.is_empty() {
                let all_entries = block_on(reader.get_range(.., ..)).unwrap();

                assert_eq!(all_entries.len(), ref_last_commit.len());

                for (blockfile_entry, expected_entry) in
                    all_entries.iter().zip(ref_last_commit.iter())
                {
                    assert_eq!(blockfile_entry.0, expected_entry.0 .1); // key matches
                    assert_eq!(blockfile_entry.1, expected_entry.1); // value matches
                }
            }

            match reader {
                BlockfileReader::ArrowBlockfileReader(reader) => {
                    assert!(block_on(reader.is_valid())) // check block sizes and the sparse index
                }
                _ => unreachable!(),
            }
        }
    }

    prop_state_machine! {
        #![proptest_config(Config {
            // verbose: 2,
            cases: 1000, // default is 256, we increase it because this test is relatively fast
            max_local_rejects: u32::MAX, // the transition precondition can reject many transitions since mutations must be applied in strictly increasing order in ordered writing mode, so we disable this limit
            ..Config::default()
        })]

        #[test]
        fn blockfile_writer_test(
            sequential
            // The number of transitions to be generated for each case.
            1..100usize
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            BlockfileWriterWrapper
        );
    }
}
