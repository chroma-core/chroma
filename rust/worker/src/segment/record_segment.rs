use super::types::{OffsetIdAssigner, SegmentWriter};
use crate::blockstore::{provider::BlockfileProvider, Blockfile};
use crate::blockstore::{KeyType, ValueType};
use crate::types::{EmbeddingRecord, Segment};
use std::sync::atomic::AtomicU32;

struct RecordSegment {
    user_id_to_id: Box<dyn Blockfile>,
    // TODO: Think about how to make the reverse mapping cheaper
    id_to_user_id: Box<dyn Blockfile>,
    records: Box<dyn Blockfile>,
    /*  TODO: store this in blockfile somehow:
         - options
            - in blockfile metadata (good)
            - in a separate file (bad)
            - special prefix in the blockfile (meh)
    */
    current_max_offset_id: AtomicU32,
}

impl RecordSegment {
    pub fn new(mut blockfile_provider: Box<dyn BlockfileProvider>) -> Self {
        // TODO: file naming etc should be better here (use segment prefix etc.)
        // TODO: move file names to consts

        let user_id_to_id =
            blockfile_provider.create("user_id_to_offset_id", KeyType::String, ValueType::Uint);
        let id_to_user_id =
            blockfile_provider.create("offset_id_to_user_id", KeyType::Uint, ValueType::String);
        // TODO: add embedding record as a value type
        let records =
            blockfile_provider.create("record", KeyType::Uint, ValueType::EmbeddingRecord);

        match (user_id_to_id, id_to_user_id, records) {
            (Ok(user_id_to_id), Ok(id_to_user_id), Ok(records)) => RecordSegment {
                user_id_to_id,
                id_to_user_id,
                records,
                current_max_offset_id: AtomicU32::new(0),
            },
            // TODO: prefer to error out here
            _ => panic!("Failed to create blockfiles"),
        }
    }

    pub fn from_segment(segment: &Segment, blockfile_provider: Box<dyn BlockfileProvider>) -> Self {
        todo!()
    }
}

impl SegmentWriter for RecordSegment {
    fn begin_transaction(&self) {
        todo!()
    }

    fn write_records(
        &self,
        records: Vec<Box<crate::types::EmbeddingRecord>>,
        offset_ids: Vec<u32>,
    ) {
        todo!()
    }

    fn commit_transaction(&self) {
        todo!()
    }

    fn rollback_transaction(&self) {
        todo!()
    }
}

impl OffsetIdAssigner for RecordSegment {
    fn assign_offset_ids(&self, records: Vec<Box<EmbeddingRecord>>) -> Vec<u32> {
        todo!()
    }
}
