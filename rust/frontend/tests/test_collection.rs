use proptest_helpers::{
    frontend_reference::{FrontendReferenceState, FrontendReferenceStateMachine},
    frontend_under_test::FrontendUnderTest,
    proptest_types::CollectionRequest,
};
use proptest_state_machine::prop_state_machine;
mod proptest_helpers;

prop_state_machine! {
     #![proptest_config(proptest::test_runner::Config {
            fork: false,
            // verbose: 2,
            ..proptest::test_runner::Config::default()
        })]
    #[test]
    fn test_collection_sqlite(sequential 1..30usize => FrontendUnderTest);
}
