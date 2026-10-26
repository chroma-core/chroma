use super::super::super::super::{
    engine::{EngineRequest, EngineResponse},
    transport::{RequestClass, RpcClient, RpcServer, TransportConfig},
};
use super::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn fixture(queries: usize) -> Vec<ExpandReply> {
    (0..queries)
        .map(|q| {
            let nodes = (0..2)
                .map(|p| NodeRef {
                    id: u128::MAX - 100 - p,
                    authority: 2,
                    placement: u64::MAX,
                })
                .collect::<Vec<_>>();
            let children = nodes
                .iter()
                .flat_map(|parent| {
                    (0..32).map(move |ordinal| {
                        let bits = [0, 0x80000000, 1, 0x807fffff, 0x3f800001, 0x7f7fffff]
                            [(q + ordinal) % 6];
                        ScoredChild {
                            parent: parent.id,
                            ordinal,
                            target: NodeRef {
                                id: u128::MAX
                                    - 1000
                                    - parent.id.wrapping_add(ordinal as u128) % 100,
                                authority: if ordinal % 2 == 0 { 2 } else { u64::MAX },
                                placement: u64::MAX - 1,
                            },
                            leaf: ordinal % 3 != 0,
                            distance: f32::from_bits(bits),
                            observed_revision: u64::MAX - 2,
                        }
                    })
                })
                .collect();
            ExpandReply {
                authority: 2,
                run: u128::MAX,
                epoch: u64::MAX,
                shared_sequence: u64::MAX - 1,
                child_counts: nodes.iter().map(|n| (n.id, 32)).collect(),
                navigation: u128::MAX - q as u128,
                step: 2,
                query_slot: q,
                nodes,
                status: ExpansionStatus::Complete,
                children,
            }
        })
        .collect()
}
fn exact(actual: &[ExpandReply], expected: &[ExpandReply]) {
    assert_eq!(actual, expected);
    for (a, b) in actual.iter().zip(expected) {
        assert_eq!(
            a.children
                .iter()
                .map(|r| r.distance.to_bits())
                .collect::<Vec<_>>(),
            b.children
                .iter()
                .map(|r| r.distance.to_bits())
                .collect::<Vec<_>>()
        );
    }
}
#[derive(Serialize, Deserialize)]
enum V1Response {
    Expanded(Vec<ExpandReply>),
}
fn unpack(response: EngineResponse) -> Vec<ExpandReply> {
    let EngineResponse::Expanded(rows) = response else {
        panic!("expanded")
    };
    rows
}
fn roundtrip(rows: &[ExpandReply]) -> Vec<ExpandReply> {
    unpack(
        serde_json::from_slice(
            &serde_json::to_vec(&EngineResponse::Expanded(rows.to_vec())).unwrap(),
        )
        .unwrap(),
    )
}

#[derive(Default)]
struct Collision;
impl Hasher for Collision {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}
#[test]
fn expansion_batch_exact_images_scores_order_and_hash_collision_equality() {
    let mut rows = fixture(64);
    // Different revision and same-revision changed metadata both require new entries.
    for row in rows[1].children.iter_mut().take(32) {
        row.observed_revision -= 1;
    }
    rows[2].children[0].target.placement -= 1;
    rows[3].children[..32].reverse();
    // Interleaved runs, partial coverage, duplicate ordinal and mixed revisions
    // remain exactly representable; the semantic validator must still reject them.
    rows[4].children.swap(0, 32);
    rows[5].children.pop();
    rows[6].children[1].ordinal = 0;
    rows[7].children[1].observed_revision -= 2;
    rows[8].children[0].ordinal = usize::MAX;
    rows[9].children[0].parent = u128::MAX;
    rows[9].children[0].observed_revision = u64::MAX;
    let wire = build(&rows, std::hash::BuildHasherDefault::<Collision>::default()).unwrap();
    assert!(u32::from_le_bytes(wire.images[..4].try_into().unwrap()) > 2);
    exact(&restore(wire).unwrap(), &rows);
    exact(&roundtrip(&rows), &rows);
    exact(&roundtrip(&[]), &[]);
    let mut empty = fixture(1);
    empty[0].children.clear();
    empty[0].status = ExpansionStatus::Stale;
    exact(&roundtrip(&empty), &empty);
    //64 is current sender policy, not a codec semantic cap.
    let mut many = fixture(1);
    many[0].children.clear();
    let many = vec![many.remove(0); 65];
    exact(&roundtrip(&many), &many);
}

#[test]
fn expansion_batch_rejects_malformed_dictionary_runs_and_nonfinite_scores() {
    let wire = build(&fixture(1), std::collections::hash_map::RandomState::new()).unwrap();
    for fault in 0..10 {
        let mut bad = wire.clone();
        match fault {
            0 => {
                bad.images.pop();
            }
            1 => {
                bad.images.push(0);
            }
            2 => bad.images[..4].copy_from_slice(&u32::MAX.to_le_bytes()),
            3 => bad.images[4 + IMAGE_HEADER + 40] = 2,
            4 => {
                bad.replies[0].runs.pop();
            }
            5 => {
                bad.replies[0].runs.push(0);
            }
            6 => bad.replies[0].runs[4..8].copy_from_slice(&u32::MAX.to_le_bytes()),
            7 => bad.replies[0].runs[8..12].copy_from_slice(&f32::INFINITY.to_bits().to_le_bytes()),
            8 => bad.replies[0].runs[..4].copy_from_slice(&u32::MAX.to_le_bytes()),
            _ => {
                let entry = bad.images[4..4 + IMAGE_HEADER + 32 * META_BYTES].to_vec();
                bad.images.extend(entry);
                bad.images[..4].copy_from_slice(&3u32.to_le_bytes());
            }
        }
        assert!(restore(bad).is_err(), "fault={fault}");
    }
    let mut unused = wire.clone();
    unused.replies[0].runs.truncate(4 + 4 + 32 * 4);
    unused.replies[0].runs[..4].copy_from_slice(&1u32.to_le_bytes());
    assert!(restore(unused).unwrap_err().contains("unused"));
    for score in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let mut rows = fixture(1);
        rows[0].children[0].distance = score;
        assert!(serde_json::to_vec(&EngineResponse::Expanded(rows)).is_err());
    }
    // All raw fields retain the strict existing base64 visitor.
    let json = r#"{"images_le_v2_base64":"!AAA","replies":[]}"#;
    assert!(serde_json::from_str::<Wire>(json).is_err());
}

#[test]
fn expansion_batch_repeated_refs_cannot_amplify_decoded_row_budget() {
    let mut wire = build(&fixture(1), std::collections::hash_map::RandomState::new()).unwrap();
    // Keep just one32-row image, then reference it thousands of times. The
    // score bytes are small enough for the wire cap, but rich rows exceed it.
    wire.images.truncate(4 + IMAGE_HEADER + 32 * META_BYTES);
    wire.images[..4].copy_from_slice(&1u32.to_le_bytes());
    let runs = MAX_ROWS / 32 + 1;
    wire.replies[0].runs = (runs as u32).to_le_bytes().to_vec();
    for _ in 0..runs {
        wire.replies[0].runs.extend_from_slice(&0u32.to_le_bytes());
        wire.replies[0].runs.extend_from_slice(&[0; 32 * 4]);
    }
    assert!(wire.images.len() + wire.replies[0].runs.len() < MAX_PAYLOAD_BYTES / 4);
    assert_eq!(restore(wire).unwrap_err(), SIZE);
    let mut large = fixture(1);
    large[0].children = vec![large[0].children[0].clone(); MAX_ROWS + 1];
    assert!(serde_json::to_vec(&EngineResponse::Expanded(large))
        .unwrap_err()
        .to_string()
        .contains(SIZE));
}

#[test]
fn expansion_batch_version_mismatch_fails_closed_and_tcp_bytes_match() {
    let rows = fixture(64);
    let cpu_before = chroma_index::spann::utils::clustering_thread_cpu_nanos();
    let old = serde_json::to_vec(&Ok::<_, String>(V1Response::Expanded(rows.clone()))).unwrap();
    let old_cpu = cpu_before
        .zip(chroma_index::spann::utils::clustering_thread_cpu_nanos())
        .map(|(a, b)| b - a);
    let cpu_before = chroma_index::spann::utils::clustering_thread_cpu_nanos();
    let new = serde_json::to_vec(&Ok::<_, String>(EngineResponse::Expanded(rows.clone()))).unwrap();
    let new_cpu = cpu_before
        .zip(chroma_index::spann::utils::clustering_thread_cpu_nanos())
        .map(|(a, b)| b - a);
    assert!(serde_json::from_slice::<Result<EngineResponse, String>>(&old).is_err());
    assert!(serde_json::from_slice::<Result<V1Response, String>>(&new).is_err());
    assert!(
        new.len() * 5 < old.len(),
        "fixed shared metadata should reduce complete framed bytes"
    );
    let response = rows.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "batch-codec".into(),
        TransportConfig::default(),
        Arc::new(move |_: EngineRequest| Ok(EngineResponse::Expanded(response.clone()))),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "batch-codec".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let actual: EngineResponse = client
        .call(
            &EngineRequest::CurrentRoot,
            RequestClass::Control,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    exact(&unpack(actual), &rows);
    let actual_bytes = client.counters().bytes_received;
    let payload_bytes = new.len() as u64 + 4;
    if crate::hierarchical_index::distributed::observability::detailed() {
        // Detailed replies include bounded server timings outside the payload.
        assert!(actual_bytes > payload_bytes && actual_bytes <= payload_bytes + 512);
    } else {
        assert_eq!(actual_bytes, payload_bytes);
    }
    eprintln!("metadata dictionary fixed64x2x32: old_framed_bytes={}, new_framed_bytes={}, old_encode_thread_cpu_nanos={old_cpu:?}, new_encode_thread_cpu_nanos={new_cpu:?}",old.len()+4,new.len()+4);
    // New wire responses fail at a legacy decoder, even though requests are
    // unchanged. A new decoder likewise refuses an old response variant.
    assert!(client
        .call::<_, V1Response>(
            &EngineRequest::CurrentRoot,
            RequestClass::Control,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    server.shutdown();
    let old_rows = rows.clone();
    let mut legacy = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "old-batch".into(),
        TransportConfig::default(),
        Arc::new(move |_: EngineRequest| Ok(V1Response::Expanded(old_rows.clone()))),
    )
    .unwrap();
    let legacy_client = RpcClient::new(
        legacy.data_addr(),
        legacy.control_addr(),
        "old-batch".into(),
        TransportConfig::default(),
    )
    .unwrap();
    assert!(legacy_client
        .call::<_, EngineResponse>(
            &EngineRequest::CurrentRoot,
            RequestClass::Control,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    legacy.shutdown();
}
