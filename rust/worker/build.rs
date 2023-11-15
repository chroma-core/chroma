fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../../idl/chromadb/proto/chroma.proto")?;
    cc::Build::new()
        .cpp(true)
        .file("bindings.cpp")
        .flag("-std=c++11")
        .flag("-Ofast")
        .flag("-DHAVE_CXX0X")
        .flag("-fpic")
        .flag("-ftree-vectorize")
        .flag("-arch arm64")
        .flag("-Rpass=loop-vectorize")
        // .flag("-DHAVE_CXX0X -openmp -fpic -ftree-vectorize -arch arm64 -isysroot /Library/Developer/CommandLineTools/SDKs/MacOSX13.1.sdk -mmacosx-version-min=13.0")
        // TODO: add other needed flags
        .compile("bindings");
    Ok(())
}
