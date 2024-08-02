mod chroma_proto {
    tonic::include_proto!("chroma");
}

// Reeexport the generated proto module
pub use chroma_proto::*;
