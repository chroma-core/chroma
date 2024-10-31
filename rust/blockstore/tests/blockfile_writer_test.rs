/// This test uses the proptest-state-machine crate to generate a sequence of transitions for a blockfile writer and compares the result after every commit with a reference implementation.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashSet, VecDeque};

    use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
    use chroma_blockstore::arrow::Block;
    use chroma_blockstore::{
        BlockfileWriter, BlockfileWriterMutationOrdering, BlockfileWriterOptions,
    };
    use chroma_storage::local::LocalStorage;
    use chroma_storage::Storage;
    use futures::executor::block_on;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use proptest_state_machine::prop_state_machine;
    use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
    use uuid::Uuid;

    use chroma_cache::new_cache_for_test;
    use itertools::Itertools;

    /// Possible transitions for our state machine (maps to `.set()`, `.delete()`, and `.commit()` on the blockfile writer).
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum Transition {
        Set(String, String, String),
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

    impl PartialOrd for Transition {
        /// The subset of Set and Delete transitions are ordered based on their prefix and key.
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            let self_prefix_and_key = self.get_prefix_and_key();
            let other_prefix_and_key = other.get_prefix_and_key();

            match (self_prefix_and_key, other_prefix_and_key) {
                (Some((self_prefix, self_key)), Some((other_prefix, other_key))) => {
                    if self_prefix == other_prefix {
                        self_key.partial_cmp(other_key)
                    } else {
                        self_prefix.partial_cmp(other_prefix)
                    }
                }
                _ => None,
            }
        }
    }

    /// This is the reference implementation of the blockfile writer that we compare against.
    #[derive(Clone)]
    pub struct RefState {
        /// This field is not used in the reference impl, but gives a block size to the real blockfile impl
        generated_max_block_size_bytes: usize,
        /// This field is not used in the reference impl, but gives a mutation ordering to the real blockfile impl
        generated_mutation_ordering: BlockfileWriterMutationOrdering,
        next_transitions: VecDeque<Transition>,
        dirty_keys: HashSet<(String, String)>,
        store: BTreeMap<(String, String), String>,
        last_commit: Option<BTreeMap<(String, String), String>>,
    }

    impl std::fmt::Debug for RefState {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // Omit the next_transitions field from the debug output (it's large and proptest will log the set of failing transitions anyways).
            f.debug_struct("RefState")
                .field(
                    "generated_max_block_size_bytes",
                    &self.generated_max_block_size_bytes,
                )
                .field(
                    "generated_mutation_ordering",
                    &self.generated_mutation_ordering,
                )
                .field("dirty_keys", &self.dirty_keys)
                .field("store", &self.store)
                .field("last_commit", &self.last_commit)
                .finish()
        }
    }

    pub struct BlockfileWriterStateMachine {}

    // Instead of generating transitions during the test, we pre-generate them and store them in the reference implementation. This is not really how proptest should be used:
    // - more transitions than necessary will likely be generated, slowing down tests
    // - proptest will spend more time shrinking failing test cases than necessary
    //
    // However, this seemed to be the easiest solution for complex transition dependencies. (When using .prop_filter(), proptest has a hard time finding valid test cases for this specific setup.)
    const MAX_TRANSITIONS: usize = 100;

    impl ReferenceStateMachine for BlockfileWriterStateMachine {
        type State = RefState;
        type Transition = Transition;

        fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
            // Some transitions are filtered out, so we over-generate transitions to ensure there are enough valid ones left.
            const NUM_TRANSITIONS: usize = MAX_TRANSITIONS * 2;

            let mutation_ordering = prop_oneof![
                Just(BlockfileWriterMutationOrdering::Unordered),
                Just(BlockfileWriterMutationOrdering::Ordered)
            ];

            let prefix_strategy = "[0-9a-zA-Z]{1,10}";
            let key_strategy = "[0-9a-zA-Z]{1,30}";
            let value_strategy = "[0-9a-zA-Z]{1,100}";

            let set_transitions = (
                proptest::collection::vec((prefix_strategy, key_strategy), NUM_TRANSITIONS),
                proptest::collection::vec(value_strategy, NUM_TRANSITIONS),
            )
                .prop_map(|(keys, values)| {
                    keys.into_iter()
                        .zip(values)
                        .map(|((prefix, key), value)| Transition::Set(prefix, key, value))
                        .collect::<Vec<_>>()
                })
                .boxed();

            // Generate:
            // - set transitions to the same prefix
            // - set transitions to the same prefix and key
            let key_update_transitions = set_transitions
                .clone()
                .prop_flat_map(move |transitions| {
                    proptest::collection::vec(
                        (
                            proptest::sample::select(transitions),
                            proptest::option::weighted(0.5, key_strategy),
                            value_strategy,
                        ),
                        NUM_TRANSITIONS / 2,
                    )
                })
                .prop_map(|transitions| {
                    transitions
                        .into_iter()
                        .map(|(transition, new_key, value)| match transition {
                            Transition::Set(prefix, original_key, _) => {
                                Transition::Set(prefix, new_key.unwrap_or(original_key), value)
                            }
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>()
                })
                .boxed();

            // Generate deletes to existing prefix/key pairs
            let delete_transitions = set_transitions
                .clone()
                .prop_flat_map(|transitions| {
                    proptest::collection::vec(
                        prop_oneof![
                            4 => Just(None),
                            // 20% chance of deleting an existing key
                            1 => proptest::sample::select(transitions).prop_map(|transition| {
                                if let Transition::Set(prefix, key, _) = transition {
                                    Some(Transition::Delete(prefix, key))
                                } else {
                                    unreachable!()
                                }
                            })
                        ],
                        NUM_TRANSITIONS,
                    )
                })
                .boxed();

            // Mix create and update transitions
            let set_transitions = (
                (set_transitions, key_update_transitions)
                    .prop_map(|(a, b)| a.into_iter().interleave(b).collect::<Vec<_>>()),
                mutation_ordering.clone(),
            )
                .prop_flat_map(|(transitions, mutation_ordering)| {
                    if mutation_ordering == BlockfileWriterMutationOrdering::Ordered {
                        // Don't shuffle when in ordered mutation mode. (Shuffling here doesn't break anything, but unnecessarily consumes shrinking iterations for failing test cases.)
                        Just(transitions).boxed()
                    } else {
                        Just(transitions).prop_shuffle().boxed()
                    }
                });

            let commit_transitions = proptest::collection::vec(
                prop_oneof![
                    8 => Just(None),
                    // 9% chance of committing
                    1 => Just(Some(Transition::Commit))
                ],
                NUM_TRANSITIONS,
            );

            // Mix set, delete, and commit transitions
            let transitions_and_mutation_ordering = (
                set_transitions,
                delete_transitions,
                commit_transitions,
                mutation_ordering,
            )
                .prop_map(
                    |(mut set_transitions, delete_transitions, commit_transitions, ordering)| {
                        if ordering == BlockfileWriterMutationOrdering::Ordered {
                            // Sort
                            set_transitions.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        }

                        let mut transitions = set_transitions
                            .into_iter()
                            .zip(delete_transitions.into_iter().zip(commit_transitions))
                            .map(|(set, (delete, commit))| {
                                if let Some(commit) = commit {
                                    commit
                                } else if let Some(delete) = delete {
                                    delete
                                } else {
                                    set
                                }
                            })
                            .collect::<VecDeque<_>>();

                        if ordering == BlockfileWriterMutationOrdering::Ordered {
                            let mut mutated_keys_since_last_commit = HashSet::new();
                            transitions = transitions
                                .into_iter()
                                .filter(|transition| match transition {
                                    Transition::Commit => {
                                        mutated_keys_since_last_commit.clear();
                                        true
                                    }
                                    Transition::Set(prefix, key, _) => {
                                        let key = (prefix.clone(), key.clone());
                                        if mutated_keys_since_last_commit.contains(&key) {
                                            false
                                        } else {
                                            mutated_keys_since_last_commit.insert(key);
                                            true
                                        }
                                    }
                                    Transition::Delete(prefix, key) => {
                                        let key = (prefix.clone(), key.clone());
                                        if mutated_keys_since_last_commit.contains(&key) {
                                            false
                                        } else {
                                            mutated_keys_since_last_commit.insert(key);
                                            true
                                        }
                                    }
                                })
                                .collect::<VecDeque<_>>()
                        }

                        (transitions, ordering)
                    },
                )
                .boxed();

            (500..1_000usize, transitions_and_mutation_ordering) // The block size is somewhat arbitrary; the min needs to be more than the largest possible block (after padding) containing a single entry. But it should be small enough that block splitting is likely to occur.
                .prop_map(
                    |(block_size_bytes, (next_transitions, generated_mutation_ordering))| {
                        RefState {
                            generated_max_block_size_bytes: block_size_bytes,
                            generated_mutation_ordering,
                            store: BTreeMap::new(),
                            dirty_keys: HashSet::new(),
                            next_transitions,
                            last_commit: None,
                        }
                    },
                )
                .boxed()
        }

        fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
            Just(state.next_transitions.front().unwrap().clone()).boxed()
        }

        fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
            if state.generated_mutation_ordering == BlockfileWriterMutationOrdering::Ordered {
                if let Some((prefix, key)) = transition.get_prefix_and_key() {
                    // We cannot mutate the same key twice while in ordered mutation mode. In most cases this is filtered during the transition generation, but when proptest is shrinking a test case it may end up violating this invariant.
                    return !state
                        .dirty_keys
                        .contains(&(prefix.to_string(), key.to_string()));
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

            state.next_transitions.pop_front();

            state
        }
    }

    /// The "real" blockfile writer implementation that we compare against the reference.
    struct BlockfileWriterWrapper {
        storage_dir: tempfile::TempDir,
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
                provider.write::<&str, String>(
                    BlockfileWriterOptions::new()
                        .set_mutation_ordering(ref_state.generated_mutation_ordering),
                ),
            )
            .unwrap();

            BlockfileWriterWrapper {
                storage_dir,
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
                            .delete::<&str, String>(prefix.as_str(), key.as_str()),
                    )
                    .unwrap();
                }
                Transition::Commit => {
                    let id = state.writer.id();
                    let flusher = block_on(state.writer.commit::<&str, String>()).unwrap();
                    block_on(flusher.flush::<&str, String>()).unwrap();

                    state.last_blockfile_id = Some(id);
                    state.writer = block_on(
                        state.provider.write::<&str, String>(
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

            let reader = block_on(state.provider.open::<&str, &str>(&last_blockfile_id)).unwrap();

            // Check count
            assert_eq!(block_on(reader.count()).unwrap(), ref_last_commit.len());

            // Check that entries are ordered and match expected
            if let Some(min_key) = ref_last_commit.keys().next() {
                let all_entries =
                    block_on(reader.get_gte(min_key.0.as_str(), min_key.1.as_str())).unwrap();

                for (blockfile_entry, expected_entry) in
                    all_entries.iter().zip(ref_last_commit.iter())
                {
                    assert_eq!(blockfile_entry.0, expected_entry.0 .1); // key matches
                    assert_eq!(blockfile_entry.1, expected_entry.1); // value matches
                }
            }

            // Check that all blocks are under the max block size
            let mut checked_at_least_one_block = false;
            for file in std::fs::read_dir(state.storage_dir.path().join("block")).unwrap() {
                let file = file.unwrap();
                let path = file.path();
                if path.is_file() {
                    let block = Block::load_with_validation(path.to_str().unwrap(), Uuid::new_v4())
                        .unwrap();
                    assert!(block.get_size() <= ref_state.generated_max_block_size_bytes);
                    checked_at_least_one_block = true;
                }
            }

            assert!(checked_at_least_one_block);
        }
    }

    prop_state_machine! {
        #![proptest_config(Config {
            // verbose: 2,
            ..Config::default()
        })]

        #[test]
        fn blockfile_writer_test(
            sequential
            // The number of transitions to be generated for each case.
            1..MAX_TRANSITIONS
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            BlockfileWriterWrapper
        );
    }
}
