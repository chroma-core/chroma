// Conversions from protobuf to rust types.
use crate::chroma_proto;
use crate::types::{EmbeddingRecord, SeqId};
use kube::client::AuthError;
use uuid::Uuid;

pub(crate) fn from_proto_submit(
    proto_submit: chroma_proto::SubmitEmbeddingRecord,
    seq_id: SeqId,
) -> Result<EmbeddingRecord, &'static str> {
    let id = proto_submit.id;
    match Uuid::parse_str(&id) {
        Ok(record_uuid) => {
            let vector = proto_submit.vector;
        }
        Err(_) => {
            return Err("Failed to parse Uuid");
        }
    }
}
