fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compile the hnswlib bindings.
    cc::Build::new()
        .cpp(true)
        .file("bindings.cpp")
        .flag("-std=c++11")
        .flag("-Ofast")
        .flag("-DHAVE_CXX0X")
        .flag("-fpic")
        .flag("-ftree-vectorize")
        .flag("-w")
        .compile("bindings");

    Ok(())
}
