// Conversions from protobuf to rust types.
use crate::chroma_proto;
use crate::types::{EmbeddingRecord, Operation, ScalarEncoding, SeqId};
use uuid::Uuid;

pub(crate) fn from_proto_submit(
    proto_submit: chroma_proto::SubmitEmbeddingRecord,
    seq_id: SeqId,
) -> Result<EmbeddingRecord, &'static str> {
    let record_id = proto_submit.id;
    let maybe_record_id = Uuid::parse_str(&record_id);
    if maybe_record_id.is_err() {
        return Err("Failed to parse record id");
    }
    let record_uuid = maybe_record_id.unwrap();

    let maybe_op = from_proto_operation(proto_submit.operation);
    if maybe_op.is_err() {
        return Err("Failed to parse operation");
    }
    let op = maybe_op.unwrap();

    let collection_id = proto_submit.collection_id;
    let collection_uuid = Uuid::parse_str(&collection_id);
    if collection_uuid.is_err() {
        return Err("Failed to parse collection id");
    }
    let collection_uuid = collection_uuid.unwrap();

    let mut embedding: Option<Vec<f32>> = None;
    let mut encoding: Option<ScalarEncoding> = None;
    if proto_submit.vector.is_some() {
        let maybe_vector_encoding = from_proto_vector(proto_submit.vector.unwrap());
        if maybe_vector_encoding.is_err() {
            return Err("Failed to parse vector");
        }
        let (v, e) = maybe_vector_encoding.unwrap();
        embedding = Some(v);
        encoding = Some(e);
    } else {
        embedding = None;
    }

    let out_record = EmbeddingRecord {
        id: record_uuid,
        seq_id: seq_id,
        embedding: embedding,
        encoding: encoding,
        metadata: None, // TODO: implement metadata
        operation: op,
        collection_id: collection_uuid,
    };
    Ok(out_record)
}

pub(crate) fn from_proto_operation(op: i32) -> Result<Operation, &'static str> {
    let maybe_op = chroma_proto::Operation::try_from(op);
    match maybe_op {
        Ok(op) => match op {
            chroma_proto::Operation::Add => Ok(Operation::Add),
            chroma_proto::Operation::Upsert => Ok(Operation::Upsert),
            chroma_proto::Operation::Update => Ok(Operation::Update),
            chroma_proto::Operation::Delete => Ok(Operation::Delete),
            _ => Err("Invalid operation"),
        },
        Err(_) => Err("Failed to parse operation"),
    }
}

pub(crate) fn from_proto_encoding(encoding: i32) -> Result<ScalarEncoding, &'static str> {
    let maybe_encoding = chroma_proto::ScalarEncoding::try_from(encoding);
    match maybe_encoding {
        Ok(encoding) => match encoding {
            chroma_proto::ScalarEncoding::Float32 => Ok(ScalarEncoding::FLOAT32),
            chroma_proto::ScalarEncoding::Int32 => Ok(ScalarEncoding::INT32),
            _ => Err("Invalid encoding"),
        },
        Err(_) => Err("Failed to parse encoding"),
    }
}

pub(crate) fn from_proto_vector(
    proto_embedding: chroma_proto::Vector,
) -> Result<(Vec<f32>, ScalarEncoding), &'static str> {
    let vector = proto_embedding.vector;
    let encoding = proto_embedding.encoding;

    if encoding != chroma_proto::ScalarEncoding::Float32 as i32 {
        return Err("Invalid encoding");
    }

    let out_vector = vec_to_f32(&vector);
    let out_encoding = from_proto_encoding(encoding);
    match (out_vector, out_encoding) {
        (Some(vector), Ok(encoding)) => Ok((vector.to_vec(), encoding)),
        _ => Err("Failed to parse vector or encoding"),
    }
}

fn vec_to_f32(bytes: &[u8]) -> Option<&[f32]> {
    // Consumes a vector of bytes and returns a vector of f32s

    if bytes.len() % 4 != 0 {
        println!("Bytes length: {}", bytes.len());
        return None; // Return None if the length is not divisible by 4
    }

    unsafe {
        // WARNING: This will only work if the machine is little endian since
        // protobufs are little endian
        let (pre, mid, post) = bytes.align_to::<f32>();
        if pre.len() != 0 || post.len() != 0 {
            println!("Pre len: {}", pre.len());
            println!("Post len: {}", post.len());
            return None; // Return None if the bytes are not aligned
        }
        return Some(mid);
    }
}
