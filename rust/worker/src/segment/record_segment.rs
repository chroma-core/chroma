use super::types::{OffsetIdAssigner, SegmentWriter};
use crate::blockstore::{provider::BlockfileProvider, Blockfile};
use crate::blockstore::{KeyType, ValueType};
use crate::types::EmbeddingRecord;
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
    pub fn new(blockfile_provider: Box<dyn BlockfileProvider>) -> Self {
        // TODO: file naming etc should be better here

        let user_id_to_id =
            blockfile_provider.create("user_id_to_id", KeyType::Uint, ValueType::Int32);
        let id_to_user_id =
            blockfile_provider.create("id_to_user_id", KeyType::Int32, ValueType::String);
        let records =
            blockfile_provider.create("record", KeyType::Int32, ValueType::EmbeddingRecord);

        RecordSegment {
            user_id_to_id: blockfile_provider.create(
                "user_id_to_id",
                KeyType::String,
                ValueType::Int32,
            ),
            id_to_user_id: blockfile_provider.create(
                "id_to_user_id",
                KeyType::Int32,
                ValueType::String,
            ),
            record: blockfile_provider.create("record", KeyType::Int32, ValueType::EmbeddingRecord),
            current_max_offset_id: AtomicU32::new(0),
        }
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
