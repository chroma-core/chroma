use super::*;
fn fixture() -> PublishedView<BoundaryView> {
    let target = NodeRef {
        id: u128::MAX - 1,
        authority: u64::MAX,
        placement: u64::MAX - 2,
    };
    let centroid = vec![-0.0, f32::from_bits(1), -0.123046875, f32::MAX];
    let mut root = RoutingNode {
        target,
        parent: None,
        centroid: centroid.clone(),
        centroid_code: vec![0, 128, 255],
        children: vec![Child {
            target: NodeRef {
                id: u128::MAX,
                ..target
            },
            centroid,
            leaf: true,
        }],
        leaf: false,
        revision: u64::MAX,
        navigation: Arc::default(),
    };
    root.prepare_navigation().unwrap();
    PublishedView {
        sequence: u64::MAX,
        attempt: Some(AttemptId {
            authority: u64::MAX,
            sequence: u64::MAX,
        }),
        value: BoundaryView {
            root: root.clone(),
            shared: BTreeMap::from([(target.id, root)]),
            units: BTreeMap::from([(
                u128::MAX,
                NodeRef {
                    id: u128::MAX,
                    ..target
                },
            )]),
            navigation_topology: None,
        },
    }
}
#[test]
fn compact_view_preserves_all_bits_and_metadata() {
    let a = fixture();
    let bytes = pack(&a).unwrap();
    let b = unpack(&bytes).unwrap();
    assert_eq!(pack(&b).unwrap(), bytes);
    assert_eq!(a.sequence, b.sequence);
    assert_eq!(a.attempt, b.attempt);
    assert_eq!(
        a.value
            .root
            .centroid
            .iter()
            .map(|f| f.to_bits())
            .collect::<Vec<_>>(),
        b.value
            .root
            .centroid
            .iter()
            .map(|f| f.to_bits())
            .collect::<Vec<_>>()
    );
    assert_eq!(b.value.root.scored_children().unwrap().count(), 1);
}
#[test]
fn compact_view_rejects_truncation_lengths_flags_versions_and_nonfinite() {
    let bytes = pack(&fixture()).unwrap();
    for end in 0..bytes.len() {
        assert!(unpack(&bytes[..end]).is_err(), "prefix {end}");
    }
    let mut extra = bytes.clone();
    extra.push(0);
    assert!(unpack(&extra).is_err());
    let mut version = bytes.clone();
    version[3] = b'2';
    assert!(unpack(&version).is_err());
    let mut flag = bytes.clone();
    flag[12] = 2;
    assert!(unpack(&flag).is_err());
    // Header, sequence, optional attempt, target reference and parent flag.
    let count_offset = 4 + 8 + 1 + 16 + 32 + 1;
    let mut count = bytes.clone();
    count[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(unpack(&count).is_err());
    let mut nan = bytes.clone();
    nan[count_offset + 4..count_offset + 8].copy_from_slice(&f32::NAN.to_bits().to_le_bytes());
    assert!(unpack(&nan).is_err());
    let mut bad = fixture();
    bad.value.root.centroid[0] = f32::INFINITY;
    assert!(pack(&bad).is_err());
    assert!(unpack(&vec![0; MAX + 1]).is_err());
}
#[test]
fn compact_view_rejects_duplicate_map_entries_and_dimension_mismatch() {
    let a = fixture();
    let bytes = pack(&a).unwrap();
    let mut r = Reader(&bytes);
    r.take(4 + 8 + 1 + 16).unwrap();
    r.node().unwrap();
    let count_offset = bytes.len() - r.0.len();
    assert_eq!(r.count(70).unwrap(), 1);
    let start = bytes.len() - r.0.len();
    r.u128().unwrap();
    r.node().unwrap();
    let end = bytes.len() - r.0.len();
    let mut dup = bytes[..count_offset].to_vec();
    dup.extend_from_slice(&2u32.to_le_bytes());
    dup.extend_from_slice(&bytes[start..end]);
    dup.extend_from_slice(&bytes[start..]);
    assert!(unpack(&dup).is_err());
    let mut bad = a;
    bad.value.root.children[0].centroid.pop();
    assert!(unpack(&pack(&bad).unwrap()).is_err());
}

#[test]
fn compact_view_rejects_wide_parent_empty_children_before_packing_navigation() {
    let mut bad = fixture();
    bad.value.root.centroid = vec![0.0; 100_000];
    bad.value.root.children = vec![
        Child {
            target: bad.value.root.target,
            centroid: vec![],
            leaf: true
        };
        10_000
    ];
    let payload = pack(&bad).unwrap();
    assert!(payload.len() < 1024 * 1024);
    assert_eq!(
        unpack(&payload).unwrap_err(),
        "invalid shared view navigation dimensions"
    );
}

#[test]
fn compact_view_rejects_invalid_and_oversized_encoded_fields() {
    for field in ["!invalid!".to_string(), "A".repeat(ENCODED_MAX + 4)] {
        let json = serde_json::to_string(&field).unwrap();
        let mut de = serde_json::Deserializer::from_str(&json);
        assert!(deserialize(&mut de).is_err());
    }
}

#[test]
fn compact_view_adapter_preserves_full_run_and_keeps_plain_view_format() {
    use super::super::PlacementRequest;
    let view = fixture();
    let ordinary = serde_json::to_string(&view).unwrap();
    assert!(ordinary.starts_with('{'));
    assert!(ordinary.contains("\"centroid\":["));
    let request = PlacementRequest::InstallView {
        run: u128::MAX,
        view,
    };
    let json = serde_json::to_string(&request).unwrap();
    assert!(json.contains("\"view\":\""));
    let decoded: PlacementRequest = serde_json::from_str(&json).unwrap();
    let PlacementRequest::InstallView { run, view } = decoded else {
        panic!("wrong request")
    };
    assert_eq!(run, u128::MAX);
    assert_eq!(pack(&view).unwrap(), pack(&fixture()).unwrap());
}
