// Conversions from protobuf to rust types.
use crate::chroma_proto;
use crate::types::{
    Collection, EmbeddingRecord, Metadata, MetadataValue, Operation, ScalarEncoding, Segment,
    SegmentScope, SeqId, UpdateMetadata, UpdateMetadataValue,
};
use uuid::Uuid;

pub(crate) fn from_proto_collection(
    proto_collection: chroma_proto::Collection,
) -> Result<Collection, &'static str> {
    let collection_id = proto_collection.id;
    let collection_uuid = Uuid::parse_str(&collection_id);
    if collection_uuid.is_err() {
        return Err("Failed to parse collection id");
    }
    return Ok(Collection {
        id: collection_uuid.unwrap(),
        name: proto_collection.name,
        topic: proto_collection.topic,
        metadata: None, // TODO: implement metadata
        dimension: proto_collection.dimension,
        tenant: proto_collection.tenant,
        database: proto_collection.database,
    });
}

pub(crate) fn from_proto_segment(
    proto_segment: chroma_proto::Segment,
) -> Result<Segment, &'static str> {
    let segment_id = proto_segment.id;
    let segment_uuid = Uuid::parse_str(&segment_id);
    if segment_uuid.is_err() {
        return Err("Failed to parse segment id");
    }
    let segment_uuid = segment_uuid.unwrap();

    let collection_id = proto_segment.collection;
    if collection_id.is_none() {
        return Err("No collection id for the given segment");
    }

    let collection_uuid = Uuid::parse_str(&collection_id.unwrap());
    if collection_uuid.is_err() {
        return Err("Failed to parse collection id");
    }
    let collection_uuid = collection_uuid.unwrap();

    let mut metadata = None;
    if proto_segment.metadata.is_some() {
        let maybe_metadata = from_proto_metadata(proto_segment.metadata.unwrap());
        if maybe_metadata.is_err() {
            return Err("Failed to parse metadata");
        }
        metadata = Some(maybe_metadata.unwrap());
    }

    let scope = from_proto_segment_scope(proto_segment.scope);
    match scope {
        Ok(scope) => Ok(Segment {
            id: segment_uuid,
            r#type: proto_segment.r#type,
            scope: scope,
            topic: proto_segment.topic,
            collection: Some(collection_uuid),
            metadata: metadata,
        }),
        Err(e) => Err(e),
    }
}

pub(crate) fn from_proto_segment_scope(proto_scope: i32) -> Result<SegmentScope, &'static str> {
    let converted_proto_scope = chroma_proto::SegmentScope::try_from(proto_scope);
    match converted_proto_scope {
        Ok(scope) => match scope {
            chroma_proto::SegmentScope::Vector => Ok(SegmentScope::VECTOR),
            chroma_proto::SegmentScope::Metadata => Ok(SegmentScope::METADATA),
            _ => Err("Invalid segment scope"),
        },
        Err(_) => Err("Failed to decode segment scope"),
    }
}

pub(crate) fn from_proto_metadata(
    proto_metadata: chroma_proto::UpdateMetadata,
) -> Result<Metadata, &'static str> {
    let mut metadata = Metadata::new();
    for (key, value) in proto_metadata.metadata.iter() {
        let maybe_value = from_proto_metadata_value(value);
        if maybe_value.is_err() {
            return Err("Failed to parse metadata value");
        }
        let value = maybe_value.unwrap();
        metadata.insert(key.clone(), value);
    }
    Ok(metadata)
}

pub(crate) fn from_proto_update_metadata(
    proto_metadata: chroma_proto::UpdateMetadata,
) -> Result<UpdateMetadata, &'static str> {
    let mut metadata = UpdateMetadata::new();
    for (key, value) in proto_metadata.metadata.iter() {
        let maybe_value = from_proto_update_metadata_value(value);
        if maybe_value.is_err() {
            return Err("Failed to parse metadata value");
        }
        let value = maybe_value.unwrap();
        metadata.insert(key.clone(), value);
    }
    Ok(metadata)
}

pub(crate) fn from_proto_metadata_value(
    proto_value: &chroma_proto::UpdateMetadataValue,
) -> Result<MetadataValue, &'static str> {
    match &proto_value.value {
        Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
            return Ok(MetadataValue::Int(*value as i32))
        }
        Some(chroma_proto::update_metadata_value::Value::FloatValue(value)) => {
            return Ok(MetadataValue::Float(*value))
        }
        Some(chroma_proto::update_metadata_value::Value::StringValue(value)) => {
            return Ok(MetadataValue::Str(value.clone()))
        }
        _ => return Err("Invalid metadata value"),
    };
}

pub(crate) fn from_proto_update_metadata_value(
    proto_value: &chroma_proto::UpdateMetadataValue,
) -> Result<UpdateMetadataValue, &'static str> {
    match &proto_value.value {
        Some(chroma_proto::update_metadata_value::Value::IntValue(value)) => {
            return Ok(UpdateMetadataValue::Int(*value as i32))
        }
        Some(chroma_proto::update_metadata_value::Value::FloatValue(value)) => {
            return Ok(UpdateMetadataValue::Float(*value))
        }
        Some(chroma_proto::update_metadata_value::Value::StringValue(value)) => {
            return Ok(UpdateMetadataValue::Str(value.clone()))
        }
        None => return Ok(UpdateMetadataValue::None),
        _ => return Err("Invalid metadata value"),
    };
}

pub(crate) fn from_proto_submit(
    proto_submit: chroma_proto::SubmitEmbeddingRecord,
    seq_id: SeqId,
) -> Result<EmbeddingRecord, &'static str> {
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
        id: proto_submit.id,
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
        // TODO: convert to big endian if the machine is big endian
        let (pre, mid, post) = bytes.align_to::<f32>();
        if pre.len() != 0 || post.len() != 0 {
            println!("Pre len: {}", pre.len());
            println!("Post len: {}", post.len());
            return None; // Return None if the bytes are not aligned
        }
        return Some(mid);
    }
}
