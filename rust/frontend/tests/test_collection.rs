use proptest_helpers::{
    frontend_reference::{FrontendReferenceState, FrontendReferenceStateMachine},
    frontend_under_test::FrontendUnderTest,
    proptest_types::CollectionRequest,
};
use proptest_state_machine::prop_state_machine;
mod proptest_helpers;

prop_state_machine! {
     #![proptest_config(proptest::test_runner::Config {
        // todo
            cases: 50,
        // cases: 10,
            // verbose: 2,
            ..proptest::test_runner::Config::default()
        })]
    // todo: add progress bar?
    #[test]
    fn test_collection(sequential 1..100usize => FrontendUnderTest);
}
