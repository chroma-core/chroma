use super::super::super::transport::{RequestClass, RpcClient, RpcServer, TransportConfig};
use super::super::{ExpandReply, ExpandRequest, ExpansionStatus};
use super::*;
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Debug, Serialize, Deserialize)]
struct Query(#[serde(with = "super::query")] Vec<f32>);
#[derive(Debug, Serialize, Deserialize)]
struct Rows(#[serde(with = "super::children")] Vec<ScoredChild>);
fn row(i: usize, distance: f32) -> ScoredChild {
    ScoredChild {
        parent: u128::MAX - 100,
        ordinal: i,
        target: NodeRef {
            id: u128::MAX - i as u128,
            authority: u64::MAX - 1,
            placement: u64::MAX,
        },
        leaf: i % 2 == 0,
        distance,
        observed_revision: u64::MAX - 2,
    }
}
fn bits(query: &[f32]) -> Vec<u32> {
    query.iter().map(|x| x.to_bits()).collect()
}
fn request(query: Vec<f32>) -> ExpandRequest {
    ExpandRequest {
        navigation: 17,
        step: 2,
        query_slot: 3,
        query,
        nodes: vec![NodeRef {
            id: u128::MAX - 100,
            authority: 2,
            placement: 4,
        }],
        run: 7,
        epoch: 8,
        shared_sequence: 9,
    }
}
fn reply(children: Vec<ScoredChild>) -> ExpandReply {
    ExpandReply {
        authority: 2,
        run: 7,
        epoch: 8,
        shared_sequence: 9,
        child_counts: vec![(u128::MAX - 100, children.len())],
        navigation: 17,
        step: 2,
        query_slot: 3,
        nodes: request(vec![]).nodes,
        status: ExpansionStatus::Complete,
        children,
    }
}
fn payload(bytes: &[u8]) -> String {
    serde_json::to_string(&BASE64_STANDARD.encode(bytes)).unwrap()
}

#[test]
fn expansion_codec_exact_finite_bits_full_width_identities_and_order() {
    let mut floats = vec![
        0.,
        -0.,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::MIN,
        f32::from_bits(1),
        f32::from_bits(0x807fffff),
    ];
    floats.extend(
        (0..8192u32)
            .map(|n| n.wrapping_mul(0x9e3779b9))
            .filter(|n| n & 0x7f800000 != 0x7f800000)
            .map(f32::from_bits),
    );
    let query = Query(floats.clone());
    let restored: Query = serde_json::from_slice(&serde_json::to_vec(&query).unwrap()).unwrap();
    assert_eq!(bits(&restored.0), bits(&floats));
    let mut children = floats
        .iter()
        .enumerate()
        .map(|(i, &score)| row(i, score))
        .collect::<Vec<_>>();
    children[0].ordinal = usize::MAX;
    children[0].parent = u128::MAX;
    children[0].target.authority = u64::MAX;
    let restored: Rows =
        serde_json::from_slice(&serde_json::to_vec(&Rows(children.clone())).unwrap()).unwrap();
    assert_eq!(restored.0, children);
    assert_eq!(
        restored
            .0
            .iter()
            .map(|r| r.distance.to_bits())
            .collect::<Vec<_>>(),
        bits(&floats)
    );
    assert_eq!(
        serde_json::from_slice::<Query>(&serde_json::to_vec(&Query(vec![])).unwrap())
            .unwrap()
            .0
            .len(),
        0
    );
    assert_eq!(
        serde_json::from_slice::<Rows>(&serde_json::to_vec(&Rows(vec![])).unwrap())
            .unwrap()
            .0
            .len(),
        0
    );
}

#[test]
fn expansion_codec_rejects_nonfinite_encode_and_decode() {
    for bad in [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::from_bits(0x7f800001),
    ] {
        assert!(serde_json::to_vec(&Query(vec![bad])).is_err());
        assert!(serde_json::to_vec(&Rows(vec![row(0, bad)])).is_err());
        let mut raw = 1u32.to_le_bytes().to_vec();
        raw.extend_from_slice(&bad.to_bits().to_le_bytes());
        assert!(serde_json::from_str::<Query>(&payload(&raw)).is_err());
        let encoded = serde_json::to_value(Rows(vec![row(0, 1.)])).unwrap();
        let mut raw = BASE64_STANDARD.decode(encoded.as_str().unwrap()).unwrap();
        raw[HEADER_BYTES + 57..HEADER_BYTES + 61].copy_from_slice(&bad.to_bits().to_le_bytes());
        assert!(serde_json::from_str::<Rows>(&payload(&raw)).is_err());
    }
}

#[test]
fn expansion_codec_rejects_malformed_lengths_flags_counts_and_base64() {
    for invalid in ["!AAA", "/w", "/w===", "/w= =", "/x==", "éAAA"] {
        let value = serde_json::to_string(invalid).unwrap();
        assert!(serde_json::from_str::<Query>(&value).is_err());
        assert!(serde_json::from_str::<Rows>(&value).is_err());
    }
    for bytes in [
        vec![],
        vec![0],
        vec![0; 3],
        vec![0; 5],
        u32::MAX.to_le_bytes().to_vec(),
    ] {
        assert!(serde_json::from_str::<Query>(&payload(&bytes)).is_err());
        assert!(serde_json::from_str::<Rows>(&payload(&bytes)).is_err());
    }
    let value = serde_json::to_value(Rows(vec![row(0, 0.)])).unwrap();
    let raw = BASE64_STANDARD.decode(value.as_str().unwrap()).unwrap();
    let mut truncated = raw.clone();
    truncated.pop();
    let mut trailing = raw.clone();
    trailing.push(0);
    let mut flags = raw.clone();
    flags[HEADER_BYTES + 56] = 2;
    let mut count = raw.clone();
    count[..4].copy_from_slice(&2u32.to_le_bytes());
    for bad in [truncated, trailing, flags, count] {
        assert!(serde_json::from_str::<Rows>(&payload(&bad)).is_err());
    }
    assert!(payload_len(usize::MAX, CHILD_BYTES).is_err());
    assert!(payload_len(usize::MAX / CHILD_BYTES + 1, CHILD_BYTES).is_err());
}

#[test]
fn expansion_codec_max_payload_bounds_include_same_encoded_length_oversize() {
    let count = (MAX_PAYLOAD_BYTES - HEADER_BYTES) / CHILD_BYTES;
    let rows = Rows((0..count).map(|i| row(i, -0.)).collect());
    let restored: Rows = serde_json::from_slice(&serde_json::to_vec(&rows).unwrap()).unwrap();
    assert_eq!(restored.0.len(), count);
    assert_eq!(
        restored.0.last().unwrap().distance.to_bits(),
        (-0f32).to_bits()
    );
    assert!(payload_len(count + 1, CHILD_BYTES).is_err());
    assert!(serde_json::to_vec(&Rows(vec![row(0, 0.); count + 1])).is_err());
    let max_query_count = (MAX_PAYLOAD_BYTES - HEADER_BYTES) / 4;
    let query = Query(vec![0.; max_query_count]);
    assert_eq!(
        serde_json::from_slice::<Query>(&serde_json::to_vec(&query).unwrap())
            .unwrap()
            .0
            .len(),
        max_query_count
    );
    assert!(serde_json::to_vec(&Query(vec![0.; max_query_count + 1])).is_err());
    let max = BASE64_STANDARD.encode(vec![0; MAX_PAYLOAD_BYTES]);
    let same = BASE64_STANDARD.encode(vec![0; MAX_PAYLOAD_BYTES + 1]);
    assert_eq!(max.len(), same.len());
    let longer = BASE64_STANDARD.encode(vec![0; MAX_PAYLOAD_BYTES + 3]);
    assert!(longer.len() > max.len());
    for bad in [same, longer] {
        let value = serde_json::to_string(&bad).unwrap();
        assert!(serde_json::from_str::<Query>(&value).is_err());
        assert!(serde_json::from_str::<Rows>(&value).is_err());
    }
}

// These are test-only copies of the legacy JSON field representation.
#[derive(Clone, Serialize, Deserialize)]
struct LegacyRequest {
    navigation: u128,
    step: u64,
    query_slot: usize,
    query: Vec<f32>,
    nodes: Vec<NodeRef>,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
}
impl From<&ExpandRequest> for LegacyRequest {
    fn from(r: &ExpandRequest) -> Self {
        Self {
            navigation: r.navigation,
            step: r.step,
            query_slot: r.query_slot,
            query: r.query.clone(),
            nodes: r.nodes.clone(),
            run: r.run,
            epoch: r.epoch,
            shared_sequence: r.shared_sequence,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct LegacyReply {
    authority: u64,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
    child_counts: Vec<(u128, usize)>,
    navigation: u128,
    step: u64,
    query_slot: usize,
    nodes: Vec<NodeRef>,
    status: ExpansionStatus,
    children: Vec<ScoredChild>,
}
impl From<&ExpandReply> for LegacyReply {
    fn from(r: &ExpandReply) -> Self {
        Self {
            authority: r.authority,
            run: r.run,
            epoch: r.epoch,
            shared_sequence: r.shared_sequence,
            child_counts: r.child_counts.clone(),
            navigation: r.navigation,
            step: r.step,
            query_slot: r.query_slot,
            nodes: r.nodes.clone(),
            status: r.status.clone(),
            children: r.children.clone(),
        }
    }
}
#[test]
fn expansion_codec_versioned_fields_reject_legacy_and_ambiguous_payloads() {
    let request = request(vec![0., -0., 1.25]);
    let reply = reply(vec![row(0, -0.)]);
    let new_request = serde_json::to_vec(&request).unwrap();
    let new_reply = serde_json::to_vec(&reply).unwrap();
    assert!(serde_json::from_slice::<LegacyRequest>(&new_request).is_err());
    assert!(serde_json::from_slice::<LegacyReply>(&new_reply).is_err());
    let old_request = serde_json::to_vec(&LegacyRequest::from(&request)).unwrap();
    let old_reply = serde_json::to_vec(&LegacyReply::from(&reply)).unwrap();
    assert!(serde_json::from_slice::<ExpandRequest>(&old_request).is_err());
    assert!(serde_json::from_slice::<ExpandReply>(&old_reply).is_err());
    // Appending the legacy key alongside the required new one is also invalid.
    for (bytes, field) in [(new_request, "query"), (new_reply, "children")] {
        let mut json = String::from_utf8(bytes).unwrap();
        json.pop();
        json.push_str(&format!(",\"{field}\":[]}}"));
        if field == "query" {
            assert!(serde_json::from_str::<ExpandRequest>(&json).is_err());
        } else {
            assert!(serde_json::from_str::<ExpandReply>(&json).is_err());
        }
    }
}

#[test]
fn expansion_codec_tcp_preserves_bits_reduces_framed_bytes_and_keeps_frame_limit() {
    let request = request((0..768).map(|i| (i as f32 - 384.) * 0.1234567).collect());
    let reply = reply(
        (0..1024)
            .map(|i| row(i, (i as f32 + 1.) * 0.1234567))
            .collect(),
    );
    let expected_request = request.clone();
    let expected_reply = reply.clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "expansion-codec".into(),
        TransportConfig::default(),
        Arc::new(move |r: ExpandRequest| {
            assert_eq!(bits(&r.query), bits(&expected_request.query));
            Ok(expected_reply.clone())
        }),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "expansion-codec".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let result: ExpandReply = client
        .call(
            &request,
            RequestClass::Data,
            Instant::now() + Duration::from_secs(5),
        )
        .unwrap();
    assert_eq!(result, reply);
    assert_eq!(
        bits(
            &result
                .children
                .iter()
                .map(|c| c.distance)
                .collect::<Vec<_>>()
        ),
        bits(
            &reply
                .children
                .iter()
                .map(|c| c.distance)
                .collect::<Vec<_>>()
        )
    );
    let framed = serde_json::to_vec(&Result::<_, String>::Ok(&reply))
        .unwrap()
        .len() as u64
        + 4;
    let actual_bytes = client.counters().bytes_received;
    let payload_bytes = framed;
    if crate::hierarchical_index::distributed::observability::detailed() {
        // Detailed replies include bounded server timings outside the payload.
        assert!(actual_bytes > payload_bytes && actual_bytes <= payload_bytes + 512);
    } else {
        assert_eq!(actual_bytes, payload_bytes);
    }
    let old_framed = serde_json::to_vec(&Result::<_, String>::Ok(LegacyReply::from(&reply)))
        .unwrap()
        .len() as u64
        + 4;
    let request_bytes = serde_json::to_vec(&request).unwrap().len();
    let old_request_bytes = serde_json::to_vec(&LegacyRequest::from(&request))
        .unwrap()
        .len();
    assert!(old_framed > framed * 2);
    assert!(old_request_bytes > request_bytes);
    let query_raw = HEADER_BYTES + 768 * 4;
    let rows_raw = HEADER_BYTES + 1024 * CHILD_BYTES;
    eprintln!("expansion codec: query_body_legacy={old_request_bytes}, query_body_new={request_bytes}, reply_framed_legacy={old_framed}, reply_framed_new={framed}, query_raw={query_raw}, rows_raw={rows_raw}");
    // The same production transport refuses the new payload with a lower ceiling.
    let small = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "expansion-codec".into(),
        TransportConfig {
            max_frame_bytes: 512,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(small
        .call::<_, ExpandReply>(
            &request,
            RequestClass::Data,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    // An old request cannot enter a new endpoint; no fallback path exists.
    assert!(client
        .call::<_, ExpandReply>(
            &LegacyRequest::from(&request),
            RequestClass::Data,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    server.shutdown();
    let old_reply = LegacyReply::from(&reply);
    let mut old_server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "old-expansion".into(),
        TransportConfig::default(),
        Arc::new(move |_: LegacyRequest| Ok(old_reply.clone())),
    )
    .unwrap();
    let old_client = RpcClient::new(
        old_server.data_addr(),
        old_server.control_addr(),
        "old-expansion".into(),
        TransportConfig::default(),
    )
    .unwrap();
    assert!(old_client
        .call::<_, ExpandReply>(
            &request,
            RequestClass::Data,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    // An old-shaped response is also rejected even with a matching request.
    assert!(old_client
        .call::<_, ExpandReply>(
            &LegacyRequest::from(&request),
            RequestClass::Data,
            Instant::now() + Duration::from_secs(5)
        )
        .is_err());
    old_server.shutdown();
}
