fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Tell cargo to rerun this build script if the bindings change.
    println!("cargo:rerun-if-changed=bindings.cpp");
    // Compile the hnswlib bindings.
    cc::Build::new()
        .cpp(true)
        .file("bindings.cpp")
        .flag("-std=c++11")
        .flag("-Ofast")
        .flag("-DHAVE_CXX0X")
        .flag("-fPIC")
        .flag("-ftree-vectorize")
        .flag("-w")
        .compile("bindings");

    Ok(())
}
