//! Exercise the actual saved-object response codec and TCP copy path. These
//! fixed byte fixtures establish representation work, not benchmark latency.
use super::super::{
    engine::VectorSource,
    transport::{RequestClass, RpcClient, RpcServer, TransportConfig},
};
use super::*;
use base64::{prelude::BASE64_STANDARD, Engine};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn reference(bytes: u64) -> ObjectRef {
    ObjectRef {
        sha256: None,
        path: "objects/fixture".into(),
        bytes,
        checksum: 123,
    }
}
fn chunk(bytes: Vec<u8>) -> SavedObjectChunk {
    SavedObjectChunk {
        object: reference(bytes.len() as u64),
        offset: 0,
        bytes,
        complete: true,
    }
}
fn encoded_value(encoded: &str) -> serde_json::Value {
    serde_json::json!({"object":reference(1),"offset":0,"bytes_base64":encoded,"complete":true})
}
#[derive(Serialize)]
struct LegacyChunk<'a> {
    object: ObjectRef,
    offset: u64,
    bytes: std::borrow::Cow<'a, [u8]>,
    complete: bool,
}
#[derive(Deserialize)]
#[allow(dead_code)]
struct LegacyDecoded {
    object: ObjectRef,
    offset: u64,
    bytes: Vec<u8>,
    complete: bool,
}
#[derive(Serialize)]
enum LegacyResponse<'a> {
    SavedObject(LegacyChunk<'a>),
}

#[test]
fn saved_chunk_base64_roundtrips_arbitrary_zero_and_max_bytes_and_rejects_legacy_wire() {
    for size in [0, 1, 2, 3, 256, OBJECT_CHUNK_BYTES] {
        let bytes = (0..size).map(|i| (i % 256) as u8).collect::<Vec<_>>();
        let original = chunk(bytes.clone());
        let json = serde_json::to_value(&original).unwrap();
        assert!(json.get("bytes").is_none());
        assert_eq!(
            json["bytes_base64"].as_str().unwrap().len(),
            size.div_ceil(3) * 4
        );
        let restored: SavedObjectChunk = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(restored.bytes, bytes);
        assert_eq!(
            (restored.object, restored.offset, restored.complete),
            (original.object.clone(), 0, true)
        );
        assert!(serde_json::from_value::<LegacyDecoded>(json).is_err());
        let legacy = serde_json::to_value(LegacyChunk {
            object: original.object,
            offset: 0,
            bytes: std::borrow::Cow::Borrowed(&bytes),
            complete: true,
        })
        .unwrap();
        assert!(serde_json::from_value::<SavedObjectChunk>(legacy).is_err());
    }
}

#[test]
fn saved_chunk_base64_rejects_invalid_padding_alphabet_and_both_oversize_cases() {
    for invalid in ["!AAA", "/w", "/w===", "/w= =", "/x==", "éAAA"] {
        assert!(
            serde_json::from_value::<SavedObjectChunk>(encoded_value(invalid)).is_err(),
            "accepted {invalid:?}"
        );
    }
    let max = BASE64_STANDARD.encode(vec![0; OBJECT_CHUNK_BYTES]);
    let same_encoded_length = BASE64_STANDARD.encode(vec![0; OBJECT_CHUNK_BYTES + 1]);
    assert_eq!(max.len(), same_encoded_length.len());
    assert!(
        serde_json::from_value::<SavedObjectChunk>(encoded_value(&same_encoded_length)).is_err()
    );
    let longer = BASE64_STANDARD.encode(vec![0; OBJECT_CHUNK_BYTES + 3]);
    assert!(longer.len() > max.len());
    assert!(serde_json::from_value::<SavedObjectChunk>(encoded_value(&longer)).is_err());
    assert!(serde_json::to_vec(&chunk(vec![0; OBJECT_CHUNK_BYTES + 1])).is_err());
}

#[derive(Clone, Copy)]
enum Fault {
    None,
    Identity,
    Offset,
    Complete,
    Empty,
    Data,
}
struct TcpFixture {
    dir: tempfile::TempDir,
    cluster: Cluster,
    peer: Arc<OwnerService>,
    client: RpcClient,
    server: RpcServer,
    object: ObjectRef,
    bytes: Vec<u8>,
}
impl TcpFixture {
    fn new(fault: Fault, size: usize) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bytes = (0..size).map(|i| (i % 256) as u8).collect::<Vec<_>>();
        let source = GenerationStore::open(dir.path().join("source")).unwrap();
        let object = source.put_object("fixture", &bytes).unwrap();
        let vectors: Arc<dyn VectorSource> =
            Arc::new(|_: u64| -> Result<Arc<[f32]>> { Ok(vec![0.; 2].into()) });
        let owner = |id| {
            Arc::new(OwnerService::new(
                id,
                900,
                2,
                chroma_distance::DistanceFunction::Euclidean,
                super::super::super::config::HierarchicalSpannConfig::default(),
                vectors.clone(),
            ))
        };
        let one = owner(1);
        let two = owner(2);
        let root = one.initial_root();
        one.install_initial(vec![root.clone()]).unwrap();
        two.install_initial(vec![two.initial_root()]).unwrap();
        two.handle(EngineRequest::CloseEpoch { run: 900, epoch: 0 })
            .unwrap();
        two.handle(EngineRequest::SealEpoch { run: 900, epoch: 0 })
            .unwrap();
        let endpoint = two.clone();
        // A full integer-array response exceeds this limit; base64 max chunks
        // fit without increasing the transport's configured bound.
        let config = TransportConfig {
            max_frame_bytes: 2 * OBJECT_CHUNK_BYTES,
            ..Default::default()
        };
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "saved-codec".into(),
            config.clone(),
            Arc::new(move |request: EngineRequest| {
                let mut response = endpoint.handle(request)?;
                if let EngineResponse::SavedObject(chunk) = &mut response {
                    match fault {
                        Fault::None => {}
                        Fault::Identity => chunk.object.checksum ^= 1,
                        Fault::Offset => chunk.offset += 1,
                        Fault::Complete => chunk.complete = !chunk.complete,
                        Fault::Empty => chunk.bytes.clear(),
                        Fault::Data => {
                            if !chunk.bytes.is_empty() {
                                chunk.bytes[0] ^= 1;
                            }
                        }
                    }
                }
                Ok(response)
            }),
        )
        .unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "saved-codec".into(),
            config,
        )
        .unwrap();
        let cluster = Cluster::new(one, BTreeMap::from([(2, client.clone())]), root.routing);
        Self {
            dir,
            cluster,
            peer: two,
            client,
            server,
            object,
            bytes,
        }
    }
    fn root(&self) -> String {
        self.dir.path().join("source").to_str().unwrap().into()
    }
    fn read(&self) -> Result<Vec<u8>> {
        remote_bytes(&self.cluster, 2, &self.root(), &self.object, 900, 0)
    }
}
impl Drop for TcpFixture {
    fn drop(&mut self) {
        self.server.shutdown();
    }
}

#[test]
fn saved_chunk_tcp_multi_chunk_bytes_and_framed_reduction_match_legacy_representation() {
    let fixture = TcpFixture::new(Fault::None, OBJECT_CHUNK_BYTES + 17);
    let copied = fixture.read().unwrap();
    assert_eq!(copied, fixture.bytes);
    let mut expected_framed = 0;
    let mut legacy_framed = 0;
    for (index, bytes) in fixture.bytes.chunks(OBJECT_CHUNK_BYTES).enumerate() {
        let offset = index as u64 * OBJECT_CHUNK_BYTES as u64;
        let complete = offset + bytes.len() as u64 == fixture.object.bytes;
        let response = EngineResponse::SavedObject(SavedObjectChunk {
            object: fixture.object.clone(),
            offset,
            bytes: bytes.to_vec(),
            complete,
        });
        expected_framed += serde_json::to_vec(&std::result::Result::<_, String>::Ok(response))
            .unwrap()
            .len() as u64
            + 4;
        let legacy = LegacyResponse::SavedObject(LegacyChunk {
            object: fixture.object.clone(),
            offset,
            bytes: std::borrow::Cow::Borrowed(bytes),
            complete,
        });
        legacy_framed += serde_json::to_vec(&std::result::Result::<_, String>::Ok(legacy))
            .unwrap()
            .len() as u64
            + 4;
    }
    assert_eq!(fixture.client.counters().messages_received, 2);
    let actual_bytes = fixture.client.counters().bytes_received;
    let payload_bytes = expected_framed;
    if crate::hierarchical_index::distributed::observability::detailed() {
        // Detailed replies include bounded server timings outside the payload.
        assert!(actual_bytes > payload_bytes && actual_bytes <= payload_bytes + 1024);
    } else {
        assert_eq!(actual_bytes, payload_bytes);
    }
    assert!(legacy_framed > 2 * expected_framed);
    let destination = GenerationStore::open(fixture.dir.path().join("destination")).unwrap();
    assert_eq!(
        copy_saved_object(&destination, &fixture.object, &copied).unwrap(),
        fixture.object
    );
    assert_eq!(
        destination.read_object(&fixture.object).unwrap(),
        fixture.bytes
    );
    // An immutable read replay returns identical decoded bytes and frame sizes.
    let before_replay = fixture.client.counters();
    assert_eq!(fixture.read().unwrap(), fixture.bytes);
    let replay_bytes = fixture.client.counters().bytes_received - before_replay.bytes_received;
    if crate::hierarchical_index::distributed::observability::detailed() {
        assert!(replay_bytes > expected_framed && replay_bytes <= expected_framed + 1024);
    } else {
        assert_eq!(replay_bytes, expected_framed);
    }
}

#[test]
fn saved_chunk_tcp_preserves_response_admission_and_whole_object_integrity_checks() {
    for fault in [
        Fault::Identity,
        Fault::Offset,
        Fault::Complete,
        Fault::Empty,
    ] {
        let fixture = TcpFixture::new(fault, 256);
        assert!(fixture
            .read()
            .unwrap_err()
            .contains("incompatible saved object chunk"));
    }
    let fixture = TcpFixture::new(Fault::Data, 256);
    // Data corruption is still valid canonical base64. Detect it at the same
    // whole-object checksum boundary used by the real coordinator save path.
    let altered = fixture.read().unwrap();
    assert_ne!(altered, fixture.bytes);
    let destination = GenerationStore::open(fixture.dir.path().join("destination")).unwrap();
    assert!(copy_saved_object(&destination, &fixture.object, &altered)
        .unwrap_err()
        .contains("integrity mismatch"));
    assert!(destination.latest().unwrap().is_none());
    assert!(!destination.root().join("descriptor.json").exists());
    // The source remains unchanged; invalid request bounds/phase still fail.
    assert_eq!(fixture.peer.owner, 2);
    for (run, epoch, offset, max_bytes) in [
        (901, 0, 0, 1),
        (900, 1, 0, 1),
        (900, 0, fixture.object.bytes + 1, 1),
        (900, 0, 0, 0),
        (900, 0, 0, OBJECT_CHUNK_BYTES + 1),
    ] {
        let request = EngineRequest::ReadSavedObject(ReadSavedObjectRequest {
            run,
            epoch,
            store_root: fixture.root(),
            object: fixture.object.clone(),
            offset,
            max_bytes,
        });
        assert!(fixture
            .client
            .call::<_, EngineResponse>(
                &request,
                RequestClass::Data,
                Instant::now() + Duration::from_secs(2)
            )
            .is_err());
    }
}
