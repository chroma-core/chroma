use super::types::{OffsetIdAssigner, SegmentWriter};
use crate::blockstore::{provider::BlockfileProvider, Blockfile};
use crate::blockstore::{BlockfileKey, Key, KeyType, Value, ValueType};
use crate::types::{LogRecord, Operation, Segment};
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
    commited_max_offset_id: AtomicU32,
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
        let records =
            blockfile_provider.create("record", KeyType::Uint, ValueType::EmbeddingRecord);

        match (user_id_to_id, id_to_user_id, records) {
            (Ok(user_id_to_id), Ok(id_to_user_id), Ok(records)) => RecordSegment {
                user_id_to_id,
                id_to_user_id,
                records,
                current_max_offset_id: AtomicU32::new(0),
                commited_max_offset_id: AtomicU32::new(0),
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
    fn begin_transaction(&mut self) {
        let t1 = self.user_id_to_id.begin_transaction();
        let t2 = self.id_to_user_id.begin_transaction();
        let t3 = self.records.begin_transaction();
        match (t1, t2, t3) {
            (Ok(()), Ok(()), Ok(())) => {}
            // TODO: handle error better and add error to interface
            _ => panic!("Failed to begin transaction"),
        }
    }

    fn write_records(&mut self, records: Vec<Box<LogRecord>>, offset_ids: Vec<Option<u32>>) {
        // TODO: this should not be mut
        let mut records = records;
        let mut offset_ids = offset_ids;
        // TODO: Once this uses log chunk, we should expect invalid ADDs to already be filtered out
        // we also then can assume that UPSERTs have been converted to ADDs or UPDATEs
        for (record, offset_id) in records.drain(..).zip(offset_ids.drain(..)) {
            match record.record.operation {
                Operation::Add => {
                    // Check if the key already exists
                    let res = self.user_id_to_id.get(BlockfileKey::new(
                        "".to_string(),
                        Key::String(record.record.id.clone()),
                    ));
                    // See if its a KeyNotFound error
                    match res {
                        Ok(_) => {
                            // This is an error, but due to decoupled read/write, we silently ignore it
                            return;
                        }
                        Err(e) => match *e {
                            crate::blockstore::BlockfileError::NotFoundError => {}
                            // TODO: don't panic here
                            _ => panic!("Unexpected error"),
                        },
                    }

                    // TODO: error handling
                    let id = offset_id.unwrap();
                    // TODO: Support empty prefixes in blockfile keys
                    let res = self.user_id_to_id.set(
                        BlockfileKey::new("".to_string(), Key::String(record.record.id.clone())),
                        Value::UintValue(id),
                    );
                    // TODO: use the res
                    let res = self.id_to_user_id.set(
                        BlockfileKey::new("".to_string(), Key::Uint(id)),
                        Value::StringValue(record.record.id.clone()),
                    );
                    let res = self.records.set(
                        BlockfileKey::new("".to_string(), Key::Uint(id)),
                        Value::EmbeddingRecordValue(*record),
                    );
                }
                // TODO: support other operations
                Operation::Upsert => {
                    // Check if the key already exists
                    let res = self.user_id_to_id.get(BlockfileKey::new(
                        "".to_string(),
                        Key::String(record.record.id.clone()),
                    ));
                    // See if its a KeyNotFound error
                    match res {
                        Ok(_) => {
                            // This is an error, but due to decoupled read/write, we silently ignore it
                            return;
                        }
                        Err(e) => match *e {
                            crate::blockstore::BlockfileError::NotFoundError => {}
                            // TODO: don't panic
                            _ => panic!("Unexpected error"),
                        },
                    }
                }
                Operation::Update => {}
                Operation::Delete => {}
            }
        }
    }

    fn commit_transaction(&mut self) {
        let t1 = self.user_id_to_id.commit_transaction();
        let t2 = self.id_to_user_id.commit_transaction();
        let t3 = self.records.commit_transaction();
        match (t1, t2, t3) {
            (Ok(()), Ok(()), Ok(())) => {}
            // TODO: handle errors
            _ => panic!("Failed to commit transaction"),
        }
        self.commited_max_offset_id.store(
            self.current_max_offset_id
                .load(std::sync::atomic::Ordering::SeqCst),
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    fn rollback_transaction(&mut self) {
        todo!()
    }
}

impl OffsetIdAssigner for RecordSegment {
    fn assign_offset_ids(&mut self, records: Vec<Box<LogRecord>>) -> Vec<Option<u32>> {
        // TODO: this should happen in a transaction
        let mut offset_ids = Vec::new();
        for record in records {
            // Only ADD and UPSERT assign an offset id if the key doesn't exist
            let id = match record.record.operation {
                Operation::Add | Operation::Upsert => {
                    // Check if the key already exists
                    let res = self.user_id_to_id.get(BlockfileKey::new(
                        "".to_string(),
                        Key::String(record.record.id.clone()),
                    ));
                    // See if its a KeyNotFound error
                    match res {
                        Ok(_) => None,
                        Err(e) => match *e {
                            crate::blockstore::BlockfileError::NotFoundError => Some(
                                self.current_max_offset_id
                                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                            ),
                            // TODO: don't panic here
                            _ => panic!("Unexpected error"),
                        },
                    }
                }
                _ => None,
            };
            offset_ids.push(id);
        }
        offset_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{OperationRecord, ScalarEncoding};
    use uuid::Uuid;

    // RESUME POINT: STORE METADATA AS PROTO AND ADD A RECORD TYPE FOR INTERNAL USE. THIS RECORD TYPE IS A OPERATION NOT A VALUE

    #[test]
    fn can_write_to_segment() {
        let blockfile_provider =
            Box::new(crate::blockstore::arrow_blockfile::provider::ArrowBlockfileProvider::new());
        let mut segment = RecordSegment::new(blockfile_provider);
        segment.begin_transaction();
        let record = Box::new(LogRecord {
            log_offset: 1,
            record: OperationRecord {
                id: "test".to_string(),
                embedding: Some(vec![1.0, 2.0, 3.0]),
                encoding: Some(ScalarEncoding::FLOAT32),
                metadata: None,
                operation: Operation::Add,
            },
        });
        let records = vec![record];
        let offset_ids = segment.assign_offset_ids(records.clone());
        segment.write_records(records, offset_ids);
        segment.commit_transaction();

        let res = segment
            .records
            .get(BlockfileKey::new("".to_string(), Key::Uint(0)));
        assert!(res.is_ok());
        let res = res.unwrap();
        println!("{:?}", res);
        match res {
            Value::EmbeddingRecordValue(record) => {
                assert_eq!(record.record.id, "test");
            }
            _ => panic!("Wrong value type"),
        }
    }
}
