use bytes::Bytes;
use chroma_storage::{admissioncontrolleds3::StorageRequestPriority, GetOptions, Storage};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

extern crate wal3;

use wal3::{
    FragmentSeqNo, Garbage, LogPosition, Manifest, Snapshot, SnapshotPointer, ThrottleOptions,
};

///////////////////////////////////////////// Condition ////////////////////////////////////////////

#[allow(dead_code)]
pub enum Condition {
    PathNotExist(String),
    Manifest(ManifestCondition),
    Snapshot(SnapshotCondition),
    Fragment(FragmentCondition),
    Garbage(GarbageCondition),
}

///////////////////////////////////////// ManifestCondition ////////////////////////////////////////

#[derive(Debug)]
pub struct ManifestCondition {
    pub acc_bytes: u64,
    pub writer: String,
    pub snapshots: Vec<SnapshotCondition>,
    pub fragments: Vec<FragmentCondition>,
}

impl ManifestCondition {
    pub async fn assert(&self, storage: &Storage, prefix: &str) {
        println!("assert_postconditions: Manifest: {:#?}", self);
        let manifest = Manifest::load(&ThrottleOptions::default(), storage, prefix)
            .await
            .unwrap();
        if let Some((manifest, _)) = manifest {
            println!("manifest: {:?}", manifest);
            assert_eq!(self.acc_bytes, manifest.acc_bytes);
            assert_eq!(self.writer, manifest.writer);
            assert_eq!(self.snapshots.len(), manifest.snapshots.len());
            for (expected, actual) in self.snapshots.iter().zip(manifest.snapshots.iter()) {
                println!("snapshot:\nexpected={expected:#?}\nactual={actual:#?}");
                assert_eq!(expected.depth, actual.depth);
                expected
                    .assert(storage, prefix, &actual.path_to_snapshot)
                    .await;
            }
            assert_eq!(self.fragments.len(), manifest.fragments.len());
            for (expected, actual) in self.fragments.iter().zip(manifest.fragments.iter()) {
                assert_eq!(expected.path, actual.path);
                assert_eq!(expected.seq_no, actual.seq_no);
                assert_eq!(expected.start, actual.start.offset());
                assert_eq!(expected.limit, actual.limit.offset());
                assert_eq!(expected.num_bytes as u64, actual.num_bytes);
            }
            println!("check succeeded");
        } else {
            panic!("manifest not found");
        }
    }
}

///////////////////////////////////////// SnapshotCondition ////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct SnapshotCondition {
    pub depth: u8,
    pub writer: String,
    pub start: LogPosition,
    pub limit: LogPosition,
    pub num_bytes: u64,
    pub snapshots: Vec<SnapshotCondition>,
    pub fragments: Vec<FragmentCondition>,
}

impl SnapshotCondition {
    pub async fn assert(&self, storage: &Storage, prefix: &str, path: &str) {
        let key = format!("{prefix}/{}", path);
        let json = storage
            .get(&key, GetOptions::new(StorageRequestPriority::P0))
            .await
            .expect("post condition expects snapshot to exist");
        let snapshot = serde_json::from_slice::<Snapshot>(&json)
            .expect("post condition expects snapshot to parse as json");
        assert_eq!(self.depth, snapshot.depth);
        assert_eq!(self.writer, snapshot.writer);
        assert_eq!(self.start, snapshot.minimum_log_position());
        assert_eq!(self.limit, snapshot.limiting_log_position());
        assert_eq!(self.snapshots.len(), snapshot.snapshots.len());
        assert_eq!(self.fragments.len(), snapshot.fragments.len());
        for (expected, actual) in self.fragments.iter().zip(snapshot.fragments.iter()) {
            assert_eq!(expected.path, actual.path);
            assert_eq!(expected.seq_no, actual.seq_no);
            assert_eq!(expected.start, actual.start.offset());
            assert_eq!(expected.limit, actual.limit.offset());
            assert_eq!(expected.num_bytes as u64, actual.num_bytes);
        }
    }

    pub fn assert_snapshot_pointer(&self, snapshot: &SnapshotPointer) {
        assert_eq!(self.depth, snapshot.depth);
        assert_eq!(self.start, snapshot.start);
        assert_eq!(self.limit, snapshot.limit);
        assert_eq!(self.num_bytes, snapshot.num_bytes);
    }
}

///////////////////////////////////////// FragmentCondition ////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct FragmentCondition {
    pub path: String,
    pub seq_no: FragmentSeqNo,
    pub start: u64,
    pub limit: u64,
    pub num_bytes: usize,
    pub data: Vec<(LogPosition, Vec<u8>)>,
}

impl FragmentCondition {
    pub async fn assert(&self, storage: &Storage, prefix: &str) {
        let key = format!("{prefix}/{}", self.path);
        let parquet = storage
            .get(&key, GetOptions::new(StorageRequestPriority::P0))
            .await
            .expect("post condition expects fragment to exist");
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from_owner(parquet.to_vec()))
            .expect("post condition expects fragment to build as parquet");
        let reader = builder
            .build()
            .expect("post condition expects fragment to read as parquet");
        let mut haystack = self.data.clone();
        for batch in reader {
            let batch = batch.expect("post condition expects record batches to not error");
            let offset = batch.column_by_name("offset").unwrap();
            let body = batch.column_by_name("body").unwrap();
            let offset = offset
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap();
            let body = body
                .as_any()
                .downcast_ref::<arrow::array::BinaryArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                let offset = offset.value(i);
                let body = body.value(i);
                let mut found = false;
                #[allow(clippy::needless_range_loop)]
                for j in 0..haystack.len() {
                    if haystack[j].0.offset() == offset && haystack[j].1 == body {
                        haystack.remove(j);
                        found = true;
                        break;
                    }
                }
                if !found {
                    panic!("found data {body:?}/{offset:?} without an expectation it's there");
                }
            }
        }
        if !haystack.is_empty() {
            panic!("missing data: {haystack:#?}");
        }
    }
}

///////////////////////////////////////// GarbageCondition /////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct GarbageCondition {
    pub snapshots_to_drop: Vec<SnapshotCondition>,
    pub snapshots_to_make: Vec<SnapshotCondition>,
    pub snapshot_for_root: Option<SnapshotCondition>,
    pub fragments_to_drop_start: FragmentSeqNo,
    pub fragments_to_drop_limit: FragmentSeqNo,
    pub first_to_keep: LogPosition,
}

impl GarbageCondition {
    pub async fn assert(&self, storage: &Storage, prefix: &str) {
        println!("asserting garbage condition {self:#?}");
        let garbage = Garbage::load(&ThrottleOptions::default(), storage, prefix)
            .await
            .unwrap();
        let (garbage, _) = garbage.expect("should have a garbage file");
        println!("garbage is {garbage:#?}");
        assert_eq!(
            garbage.fragments_to_drop_start,
            self.fragments_to_drop_start
        );
        assert_eq!(
            garbage.fragments_to_drop_limit,
            self.fragments_to_drop_limit
        );
        assert_eq!(garbage.first_to_keep, self.first_to_keep);
        match (
            self.snapshot_for_root.as_ref(),
            garbage.snapshot_for_root.as_ref(),
        ) {
            (Some(lhs), Some(rhs)) => {
                println!("Considering snapshot pointer\n{:#?}\n{:#?}", lhs, rhs);
                lhs.assert_snapshot_pointer(rhs);
            }
            (None, None) => {}
            (Some(_), None) => {
                panic!("snapshot for root expected, but not set")
            }
            (None, Some(_)) => {
                panic!("snapshot for root unexpected, but set")
            }
        };
        eprintln!(
            "expected: {:#?}\nreturned: {:#?}",
            self.snapshots_to_drop, garbage.snapshots_to_drop
        );
        assert_eq!(
            garbage.snapshots_to_drop.len(),
            self.snapshots_to_drop.len()
        );
        for (lhs, rhs) in std::iter::zip(
            garbage.snapshots_to_drop.iter(),
            self.snapshots_to_drop.iter(),
        ) {
            rhs.assert_snapshot_pointer(lhs);
        }
        assert_eq!(
            garbage.snapshots_to_make.len(),
            self.snapshots_to_make.len()
        );
        for (lhs, rhs) in std::iter::zip(
            garbage.snapshots_to_make.iter(),
            self.snapshots_to_make.iter(),
        ) {
            rhs.assert_snapshot_pointer(&lhs.to_pointer());
        }
    }
}

///////////////////////////////////////// assert_conditions ////////////////////////////////////////

pub async fn assert_conditions(storage: &Storage, prefix: &str, postconditions: &[Condition]) {
    for postcondition in postconditions {
        match postcondition {
            Condition::PathNotExist(path) => {
                println!("assert_postconditions: PathNotExist: {}", path);
                assert!(matches!(
                    storage
                        .get(path, GetOptions::new(StorageRequestPriority::P0))
                        .await,
                    Err(chroma_storage::StorageError::NotFound { .. })
                ));
                println!("check succeeded");
            }
            Condition::Manifest(postcondition) => {
                postcondition.assert(storage, prefix).await;
            }
            Condition::Snapshot(_) => {
                // TODO(rescrv):  Figure out some way to fix the setsum so we can address snapshots
                // by setsum.  Otherwise addressing them by path is difficult to do in test.
                // If this is problematic, reference a snapshotcondition within a manifest and
                // it'll get tested there.
            }
            Condition::Fragment(postcondition) => {
                postcondition.assert(storage, prefix).await;
            }
            Condition::Garbage(postcondition) => {
                postcondition.assert(storage, prefix).await;
            }
        }
    }
}
