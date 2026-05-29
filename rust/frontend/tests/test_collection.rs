use proptest::test_runner::Config;
use proptest_helpers::{
    frontend_reference::FrontendReferenceStateMachine, frontend_under_test::FrontendUnderTest,
    proptest_types::CollectionRequest,
};
use proptest_state_machine::prop_state_machine;
mod proptest_helpers;

const FRONTEND_PROPTEST_PROFILE_ENV: &str = "CHROMA_FRONTEND_PROPTEST_PROFILE";

fn frontend_deep_profile_enabled() -> bool {
    match std::env::var(FRONTEND_PROPTEST_PROFILE_ENV) {
        Ok(profile) => profile.eq_ignore_ascii_case("deep"),
        Err(_) => false,
    }
}

fn frontend_proptest_config() -> Config {
    let deep_profile = frontend_deep_profile_enabled();

    Config {
        fork: false,
        cases: if deep_profile { 256 } else { 64 },
        max_shrink_iters: if deep_profile { u32::MAX } else { 1024 },
        // verbose: 2,
        ..Config::default()
    }
}

fn frontend_transition_range() -> std::ops::Range<usize> {
    if frontend_deep_profile_enabled() {
        1..30usize
    } else {
        1..16usize
    }
}

prop_state_machine! {
     #![proptest_config(frontend_proptest_config())]
    #[test]
    fn test_collection_sqlite(sequential frontend_transition_range() => FrontendUnderTest);
}
