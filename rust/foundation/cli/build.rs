use std::env;

fn main() {
    let version = env::var("FOUNDATION_VERSION").unwrap_or_else(|_| "dev".to_string());
    let commit = env::var("FOUNDATION_COMMIT").unwrap_or_else(|_| "none".to_string());
    let date = env::var("FOUNDATION_DATE")
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string());

    println!("cargo:rustc-env=FOUNDATION_VERSION={}", version);
    println!("cargo:rustc-env=FOUNDATION_COMMIT={}", commit);
    println!("cargo:rustc-env=FOUNDATION_DATE={}", date);
}
