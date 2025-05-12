// build.rs - UniFFI build script
fn main() {
    // Tell Cargo to invalidate the built crate whenever the schema changes
    println!("cargo:rerun-if-changed=src/chroma.udl");
    uniffi::generate_scaffolding("src/chroma.udl").unwrap();
}
