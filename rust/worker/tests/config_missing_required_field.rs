use figment::Jail;
use serial_test::serial;
use worker::config::RootConfig;

#[test]
#[should_panic]
#[serial]
fn test_config_missing_required_field() {
    Jail::expect_with(|jail| {
        let _ = jail.create_file(
            "chroma_config.yaml",
            r#"
            query_service:
                assignment_policy:
                    RendezvousHashing:
                        hasher: Murmur3
            "#,
        );
        let _ = RootConfig::load();
        Ok(())
    });
}
