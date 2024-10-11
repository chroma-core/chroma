/// This test uses the proptest-state-machine crate to generate a sequence of transitions for a blockfile writer and compares the result after every commit with a reference implementation.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chroma_storage::local::LocalStorage;
    use chroma_storage::Storage;
    use futures::executor::block_on;
    use futures::TryStreamExt;
    use proptest::prelude::*;
    use proptest::test_runner::Config;
    use proptest_state_machine::prop_state_machine;
    use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};
    use uuid::Uuid;

    use crate::arrow::provider::ArrowBlockfileProvider;
    use crate::{BlockfileWriter, BlockfileWriterOptions};
    use chroma_cache::new_cache_for_test;

    #[derive(Clone, Debug)]
    pub enum Transition {
        Set(String, String, String),
        Delete(String, String),
        Commit,
    }

    #[derive(Debug, Clone)]
    pub struct RefState {
        /// This field is not used in the reference impl, but gives a block size to the real blockfile impl
        generated_max_block_size_bytes: usize,
        store: BTreeMap<(String, String), String>,
        last_commit: Option<BTreeMap<(String, String), String>>,
    }

    pub struct BlockfileWriterStateMachine {}

    impl ReferenceStateMachine for BlockfileWriterStateMachine {
        type State = RefState;
        type Transition = Transition;

        fn init_state() -> proptest::prelude::BoxedStrategy<Self::State> {
            (500..1_000usize) // The block size is somewhat arbitrary; the min needs to be more than the largest possible block (after padding) containing a single entry. But it should be small enough that block splitting is likely to occur.
                .prop_map(|block_size_bytes| RefState {
                    generated_max_block_size_bytes: block_size_bytes,
                    store: BTreeMap::new(),
                    last_commit: None,
                })
                .boxed()
        }

        fn transitions(state: &Self::State) -> proptest::prelude::BoxedStrategy<Self::Transition> {
            let keys = state.store.keys().cloned().collect::<Vec<_>>();

            let key_and_prefix_generator = (
                "[0-9a-zA-Z]{1,10}",
                "[0-9a-zA-Z]{1,10}",
                "[0-9a-zA-Z]{1,100}",
            );

            if keys.is_empty() {
                return key_and_prefix_generator
                    .prop_map(|(prefix, key, value)| Transition::Set(prefix, key, value))
                    .boxed();
            }

            prop_oneof![
                // 57% chance of setting a new key
                4 => key_and_prefix_generator.prop_map(|(prefix, key, value)| {
                    Transition::Set(prefix, key, value)
                }),
                // 28% chance of deleting an existing key
                2 => (0..keys.len()).prop_map(move |i| {
                    Transition::Delete(keys[i].0.clone(), keys[i].1.clone())
                }),
                // 14% chance of committing
                1 => Just(Transition::Commit)
            ]
            .boxed()
        }

        fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
            match transition {
                Transition::Set(prefix, key, value) => {
                    state
                        .store
                        .insert((prefix.clone(), key.clone()), value.clone());
                }
                Transition::Delete(prefix, key) => {
                    state.store.remove(&(prefix.clone(), key.clone()));
                }
                Transition::Commit => {
                    state.last_commit = Some(state.store.clone());
                }
            }

            state
        }
    }

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
            let tmp_dir = tempfile::tempdir().unwrap();
            let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
            let block_cache = new_cache_for_test();
            let sparse_index_cache = new_cache_for_test();
            let provider = ArrowBlockfileProvider::new(
                storage,
                ref_state.generated_max_block_size_bytes,
                block_cache,
                sparse_index_cache,
            );
            let writer =
                block_on(provider.get_writer::<&str, String>(BlockfileWriterOptions::default()))
                    .unwrap();

            BlockfileWriterWrapper {
                provider,
                last_blockfile_id: None,
                writer,
            }
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            _: &<Self::Reference as proptest_state_machine::ReferenceStateMachine>::State,
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
                        state
                            .provider
                            .get_writer::<&str, String>(BlockfileWriterOptions::new().fork(id)),
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
            let all_entries =
                block_on(reader.get_range_stream(.., ..).try_collect::<Vec<_>>()).unwrap();
            assert_eq!(all_entries.len(), ref_last_commit.len());

            for (blockfile_entry, expected_entry) in all_entries.iter().zip(ref_last_commit.iter())
            {
                assert_eq!(blockfile_entry.0, expected_entry.0 .1); // key matches
                assert_eq!(blockfile_entry.1, expected_entry.1); // value matches
            }
        }
    }

    prop_state_machine! {
        #![proptest_config(Config::default())]

        #[test]
        fn blockfile_writer_test(
            sequential
            // The number of transitions to be generated for each case.
            1..100
            // Macro's boilerplate to separate the following identifier.
            =>
            // The name of the type that implements `StateMachineTest`.
            BlockfileWriterWrapper
        );
    }
}
