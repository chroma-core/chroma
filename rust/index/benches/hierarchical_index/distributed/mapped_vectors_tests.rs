use super::*;
use sha2::{Digest, Sha256};

const DIMS: usize = 5;
const FIRST_ID: u64 = 7;

fn rows() -> Vec<[f32; DIMS]> {
    (0..3)
        .map(|r| std::array::from_fn(|c| r as f32 * 10.0 + c as f32 + 0.5))
        .collect()
}

/// Writes 3 rows x 5 dims at stride 20 and returns a descriptor with the
/// correct digest.
fn corpus(dir: &Path) -> (VectorFileDescriptor, Vec<u8>) {
    let bytes: Vec<u8> = rows()
        .iter()
        .flatten()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let path = dir.join("corpus.f32");
    std::fs::write(&path, &bytes).unwrap();
    let descriptor = VectorFileDescriptor {
        path: path.to_str().unwrap().into(),
        rows: 3,
        dimensions: DIMS,
        stride_bytes: 20,
        offset_bytes: 0,
        first_id: FIRST_ID,
        file_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        immutable_locators: vec!["s3://fixture/corpus@v1".into()],
    };
    (descriptor, bytes)
}

#[test]
fn first_open_hashes_and_second_open_uses_the_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (descriptor, _) = corpus(dir.path());
    let (_, first) = MappedVectorSource::open(descriptor.clone(), None).unwrap();
    assert!(first.hashed);
    assert_eq!(
        first.receipt_path,
        dir.path().join("corpus.f32.sha256-receipt.json")
    );
    assert!(first.receipt_path.exists());
    let (_, second) = MappedVectorSource::open(descriptor, None).unwrap();
    assert!(!second.hashed);
    assert_eq!(second.hash_seconds, 0.0);
}

#[test]
fn receipt_dir_overrides_the_corpus_directory() {
    let dir = tempfile::tempdir().unwrap();
    let receipts = tempfile::tempdir().unwrap();
    let (descriptor, _) = corpus(dir.path());
    let (_, report) = MappedVectorSource::open(descriptor, Some(receipts.path())).unwrap();
    assert_eq!(
        report.receipt_path,
        receipts.path().join("corpus.f32.sha256-receipt.json")
    );
    assert!(report.receipt_path.exists());
}

#[test]
fn rows_borrow_exact_values_and_get_matches() {
    let dir = tempfile::tempdir().unwrap();
    let (descriptor, _) = corpus(dir.path());
    let (source, _) = MappedVectorSource::open(descriptor, None).unwrap();
    for (i, expected) in rows().iter().enumerate() {
        let id = FIRST_ID + i as u64;
        assert_eq!(source.row(id).unwrap(), expected.as_slice());
        assert_eq!(
            &*VectorSource::get(&source, id).unwrap(),
            expected.as_slice()
        );
    }
    assert!(source.row(FIRST_ID - 1).is_err());
    assert!(source.row(FIRST_ID + 3).is_err());
    assert!(VectorSource::get(&source, u64::MAX).is_err());
}

#[test]
fn wrong_digest_fails_and_writes_no_receipt() {
    let dir = tempfile::tempdir().unwrap();
    let (mut descriptor, _) = corpus(dir.path());
    descriptor.sha256 = "0".repeat(64);
    let err = MappedVectorSource::open(descriptor, None).err().unwrap();
    assert!(err.contains("SHA-256"), "{err}");
    assert!(!dir.path().join("corpus.f32.sha256-receipt.json").exists());
}

#[test]
fn rewriting_the_file_forces_a_rehash() {
    let dir = tempfile::tempdir().unwrap();
    let (descriptor, bytes) = corpus(dir.path());
    assert!(
        MappedVectorSource::open(descriptor.clone(), None)
            .unwrap()
            .1
            .hashed
    );
    // Same bytes, new mtime/ctime: the receipt can no longer vouch for content.
    std::thread::sleep(std::time::Duration::from_millis(20));
    std::fs::write(&descriptor.path, &bytes).unwrap();
    let (source, report) = MappedVectorSource::open(descriptor.clone(), None).unwrap();
    assert!(report.hashed);
    assert_eq!(source.row(FIRST_ID).unwrap(), rows()[0].as_slice());
    assert!(!MappedVectorSource::open(descriptor, None).unwrap().1.hashed);
}

#[test]
fn misaligned_or_invalid_descriptors_fail() {
    let dir = tempfile::tempdir().unwrap();
    let (descriptor, _) = corpus(dir.path());
    let mut misaligned = descriptor.clone();
    misaligned.stride_bytes = 22;
    misaligned.rows = 2;
    assert!(MappedVectorSource::open(misaligned, None).is_err());
    let mut offset = descriptor.clone();
    offset.offset_bytes = 2;
    offset.rows = 2;
    assert!(MappedVectorSource::open(offset, None).is_err());
    let mut length = descriptor.clone();
    length.file_bytes += 4;
    assert!(MappedVectorSource::open(length, None).is_err());
    let mut locators = descriptor;
    locators.immutable_locators.clear();
    assert!(MappedVectorSource::open(locators, None).is_err());
}

#[test]
fn resident_pages_reports_within_the_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let (descriptor, _) = corpus(dir.path());
    let (source, _) = MappedVectorSource::open(descriptor, None).unwrap();
    source.row(FIRST_ID).unwrap();
    assert!(source.resident_pages().unwrap() <= 1);
}

#[test]
fn source_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MappedVectorSource>();
}
