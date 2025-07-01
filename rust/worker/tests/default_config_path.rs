use serial_test::serial;
use worker::config::RootConfig;

#[test]
#[serial]
fn test_default_config_path() {
    // Sanity check that root config loads from default path correctly
    let _ = RootConfig::load();
}
